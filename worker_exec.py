from database.connector import open_session, close_session
from database.tables import Action, ActionProperty, Node
from datetime import datetime
from importlib import import_module
from lib.config_loader import DATE_FORMAT, load_config
from sqlalchemy import or_
import logging, os, time

# Import the action driver from config_worker.json
# Import the exec_action function
action_driver = load_config()["action_driver"]
py_module = import_module("%s.action_exec" % action_driver)
exec_action_fct = getattr(py_module, "exec_action_fct")
# Import the PROCESS and STATE_DESC variables
py_module = import_module("%s.states" % action_driver)
PROCESS = getattr(py_module, "PROCESS")
STATE_DESC = getattr(py_module, "STATE_DESC")


# Move the action to the next state of the process
def next_state_move(db_action):
    # Select the state list from the environment
    state_list = []
    for process in PROCESS[db_action.process]:
        if len(process["environments"]) == 0 or db_action.environment in process["environments"]:
            state_list = process["states"]
    if len(state_list) == 0:
        db_action.state = "lost"
        logging.error("[%s] no state list for the '%s' process and the '%s' environment" % (
            db_action.node_name, db_action.process, db_action.environment))
        return False
    # Increase the state index of the action
    if db_action.state_idx is None:
        db_action.state_idx = 0
    else:
        if (db_action.state_idx + 1) < len(state_list):
            db_action.state_idx += 1
        else:
            logging.error("[%s] no more state (index: %d) in the '%s' process" % (
                db_action.node_name, db_action.state_idx, db_action.process))
            db_action.state = "lost"
            return False
    # Set the state of the action to the next state of the process
    db_action.state = state_list[db_action.state_idx]
    db_action.updated_at = datetime.now().strftime(DATE_FORMAT)
    logging.info("[%s] changes to the '%s' state" % (db_action.node_name, db_action.state))
    return True


def init_action_process(db_action, process_name):
    db_action.process = process_name
    db_action.state_idx = None
    db_action.state = None
    next_state_move(db_action)


def load_reboot_state(db_action):
    if db_action.reboot_state is not None and len(db_action.reboot_state) > 0:
        logging.info("[%s] load the reboot state '%s'" % (db_action.node_name, db_action.reboot_state))
        process_info = db_action.reboot_state.split("?!")
        if len(process_info) == 2:
            db_action.process = process_info[0]
            idx = int(process_info[1])
            if idx == 0:
                db_action.state_idx = None
            else:
                db_action.state_idx = idx - 1
            db_action.reboot_state = None
            next_state_move(action)
        else:
            logging.error("[%s] can not find the process for the '%s' state" % (
                db_action.node_name, db_action.reboot_state))


if __name__ == "__main__":
    # This file used by the SystemD service
    STOP_FILE = "tasksstop"
    # Logging configuration
    logging.basicConfig(filename='info_exec.log', level=logging.INFO)
    # Detect the final states
    final_states = [ "lost" ]
    for state in STATE_DESC:
        if not STATE_DESC[state]["exec"] and not STATE_DESC[state]["post"]:
            final_states.append(state)
    logging.info("### Final states: %s" % final_states)
    # Try to configure the lost actions
    db = open_session()
    lost_actions = db.query(Action).filter(Action.state == "lost").all()
    for action in lost_actions:
        logging.info("[%s] lost action rescue" % action.node_name)
        load_reboot_state(action)
    close_session(db)
    # Analyzing the database
    while not os.path.isfile(STOP_FILE):
        db = open_session()
        try:
            # Load reboot_state for the actions in the 'rebooted' state or in the 'lost' state
            reboot_actions = db.query(Action).filter(Action.state == "rebooted").all()
            for action in reboot_actions:
                load_reboot_state(action)
            # Delete the actions in final states
            final_actions = db.query(Action).filter(Action.state.in_(final_states)).all()
            for action in final_actions:
                logging.info("[%s] action is completed (current state: '%s')" % (
                    action.node_name, action.state))
                # Update the node status with the action state
                node = db.query(Node).filter(Node.name == action.node_name).all()
                # As the Node.name is the primary key, only one node should be selected
                for n in node:
                    n.status = action.state
                # Delete the action
                db.delete(action)
            # Start actions for the recently configured nodes
            pending_nodes = db.query(Node).filter(Node.status == "ready").all()
            for node in pending_nodes:
                logging.info("[%s] starts the deploy process" % node.name) 
                act_prop = db.query(ActionProperty
                    ).filter(ActionProperty.node_name == node.name
                    ).filter(or_(ActionProperty.prop_name == "name", ActionProperty.prop_name == "environment")
                    ).all()
                act = Action()
                for prop in act_prop:
                    if prop.prop_name == "name":
                        act.name = prop.prop_value
                        act.node_name = node.name
                        act.node_ip = node.ip
                    if prop.prop_name == "environment":
                        act.environment = prop.prop_value
                init_action_process(act, "deploy")
                node.status = "in_progress"
                db.add(act)
            # Process the ongoing actions
            pending_actions = db.query(Action).filter(~Action.state.in_(final_states)
                ).all()
            # Sort the actions according the list of states
            sorted_actions = { key: [] for key in STATE_DESC.keys() }
            for action in pending_actions:
                action_state = action.state.replace("_exec", "").replace("_post","")
                if action_state in sorted_actions:
                    sorted_actions[action_state].append(action)
                else:
                    logging.warning("[%s] unknow state '%s'" % (action.node_name, action.state))
            # Execute the functions of the states
            for state in sorted_actions:
                for action in sorted_actions[state]:
                    state_fct = action.state
                    if not state_fct.endswith("_exec") and not state_fct.endswith("_post"):
                        if STATE_DESC[action.state]["exec"]:
                            state_fct = action.state + "_exec"
                        else:
                            state_fct = action.state + "_post"
                    logging.info("[%s] enters in '%s' state" % (action.node_name, action.state))
                    # Execute the function associated to the action state
                    if exec_action_fct(state_fct, action):
                        # Update the state of the action
                        if state_fct.endswith("_exec") and STATE_DESC[state]["post"]:
                            # Execute the '_post' function
                            action.state = state_fct.replace("_exec", "_post")
                        else:
                            # Move to the next state of the process
                            next_state_move(action)
                    else:
                        # The node is not ready, test the reboot timeout
                        if action.updated_at is None:
                            action.updated_at = datetime.now().strftime(DATE_FORMAT)
                        updated = datetime.strptime(str(action.updated_at), DATE_FORMAT)
                        elapsedTime = (datetime.now() - updated).total_seconds()
                        action_state = action.state.replace("_exec", "").replace("_post","")
                        reboot_timeout = STATE_DESC[action_state]["before_reboot"]
                        do_lost = True
                        if reboot_timeout > 0 and action.process != "reboot":
                            if elapsedTime > reboot_timeout:
                                logging.warning("[%s] hard reboot the node" % action.node_name)
                                # Remember the last state of the current process
                                action.reboot_state = "%s?!%d" % (action.process, action.state_idx)
                                init_action_process(action, "reboot")
                            else:
                                do_lost = False
                                logging.info("[%s] not ready since %d seconds" %(action.node_name, elapsedTime))
                        # The node is not ready, test the lost timeout
                        lost_timeout = STATE_DESC[action_state]["lost"]
                        if do_lost and lost_timeout > 0:
                            if elapsedTime > lost_timeout:
                                logging.warning("[%s] is lost. Stop monitoring it!" % action.node_name)
                                if action.process != "reboot":
                                    # Remember the last state of the current process
                                    action.reboot_state = "%s?!%d" % (action.process, action.state_idx)
                                action.state = "lost"
                            else:
                                logging.info("[%s] not ready since %d seconds" %(action.node_name, elapsedTime))
        except Exception as e:
            logging.exception("Node process error")
        close_session(db)
        # Waiting for the node configuration
        time.sleep(3)
    if os.path.isfile(STOP_FILE):
        os.remove(STOP_FILE)
    logging.info("### The piTasks service is stopped.")
