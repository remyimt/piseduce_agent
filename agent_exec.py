# Load the configuration file
import sys
from lib.config_loader import load_config, get_config
if len(sys.argv) != 2:
    print("The configuration file is required in parameter.")
    print("For example, 'python3 %s config.json'" % sys.argv[0])
    sys.exit(2)
load_config(sys.argv[1])

from database.connector import open_session, close_session
from database.tables import Action, ActionProperty, Node, NodeProperty
from datetime import datetime
from importlib import import_module
from lib.config_loader import DATE_FORMAT, load_config
from sqlalchemy import or_
import logging, os, subprocess, sys, time


# Import the action driver from config_agent.json
node_type = get_config()["node_type"]
# Import the action executor module
exec_action_mod = import_module("%s.action_exec" % node_type)
# Import the PROCESS and STATE_DESC variables
py_module = import_module("%s.states" % node_type)
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


def new_action(db_node, db):
    act = Action()
    act_prop = db.query(ActionProperty
        ).filter(ActionProperty.node_name == db_node.name
        ).filter(ActionProperty.prop_name == "environment"
        ).first()
    if act_prop is not None:
        act.environment = act_prop.prop_value
        act.node_name = db_node.name
        act.node_ip = db_node.ip
    else:
        logging.warning("[%s] can not create actions without detecting an associated environment" % db_node.name)
    db_node.status = "in_progress"
    return act


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
            next_state_move(db_action)
        else:
            logging.error("[%s] can not find the process for the '%s' state" % (
                db_action.node_name, db_action.reboot_state))


def load_lost_state(db_node, db):
    if db_node.lost_state is not None and len(db_node.lost_state) > 0:
        logging.info("[%s] load the lost state '%s'" % (db_node.name, db_node.lost_state))
        process_info = db_node.lost_state.split("?!")
        if len(process_info) == 2:
            # Create a new action to continue the process
            act = new_action(db_node, db)
            if len(act.node_name) > 0 and len(act.environment) > 0:
                act.process = process_info[0]
                idx = int(process_info[1])
                if idx == 0:
                    act.state_idx = None
                else:
                    act.state_idx = idx - 1
                db.add(act)
                next_state_move(act)
                db_node.lost_state = None
            else:
                logging.error("[%s] wrong configuration for the new action" % db_node.name)
        else:
            logging.error("[%s] can not find the process for the '%s' state" % (
                db_node.name, db_node.lost_state))


def free_reserved_node(db_node):
    db_node.owner = None
    db_node.bin = None
    db_node.lost_state = None
    db_node.start_date = None
    db_node.duration = None
    db_node.status = "available"


if __name__ == "__main__":
    # This file used by the SystemD service
    STOP_FILE = "execstop"
    # Logging configuration
    logging.basicConfig(filename='info_exec.log', level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # Detect the final states
    final_states = [ "lost" ]
    for state in STATE_DESC:
        if not STATE_DESC[state]["exec"] and not STATE_DESC[state]["post"]:
            final_states.append(state)
    logging.info("### Final states: %s" % final_states)
    # Connection to the database
    db = open_session()
    # Register the information about the pimaster (me)
    # Get the user account (this should be the root account)
    if "user" in get_config():
        my_user = get_config()["user"]
    else:
        cmd = "whoami"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        my_user = process.stdout.decode("utf-8").strip()
    # Get the private IP address that could be used by the agent to deploy nodes (used to deploy Raspberry)
    if "ip" in get_config():
        my_ip = get_config()["ip"]
    else:
        cmd = "hostname -i"
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        my_ip = process.stdout.decode("utf-8").split()
        if len(my_ip) > 0:
            my_ip = my_ip[0]
        if len(my_ip) == 0 or len(my_ip.split(".")) != 4:
            # Try another command
            cmd = "hostname -I"
            process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            my_ip = process.stdout.decode("utf-8").split()
            if len(my_ip) > 0:
                my_ip = my_ip[0]
    # Update the pimaster information of the database
    pimaster_info = db.query(NodeProperty).filter(NodeProperty.name == "pimaster").all()
    if len(my_user) > 0 and len(my_ip) > 0 and len(my_ip.split(".")) == 4:
        if pimaster_info is not None and len(pimaster_info) > 0:
            logging.info("Update the pimaster information from existing DB records")
            # Check the pimaster information
            for info in pimaster_info:
                if info.prop_name == "user" and info.prop_value != my_user:
                    info.prop_value = my_user
                if info.prop_name == "ip" and info.prop_value != my_ip:
                    info.prop_value = my_ip
        else:
            # Add the pimaster information in the database
            logging.info("Create new records to register the pimaster information")
            user_record = NodeProperty()
            user_record.name = "pimaster"
            user_record.prop_name = "user"
            user_record.prop_value = my_user
            db.add(user_record)
            ip_record = NodeProperty()
            ip_record.name = "pimaster"
            ip_record.prop_name = "ip"
            ip_record.prop_value = my_ip
            db.add(ip_record)
        logging.info("pimaster ip: %s, pimaster user: %s" % (my_ip, my_user))
    else:
        logging.error("can not get the IP or the user of the pimaster: ip=%s, user=%s" % (my_ip, my_user))
        sys.exit(13)
    # Try to configure the lost nodes
    lost_nodes = db.query(Node
        ).filter(Node.status == "lost"
        ).all()
    for node in lost_nodes:
        logging.info("[%s] lost node rescue" % node.name)
        load_lost_state(node, db)
    close_session(db)
    # Analyzing the database
    while not os.path.isfile(STOP_FILE):
        db = open_session()
        try:
            # Load reboot_state for the actions in the 'rebooted' state or in the 'lost' state
            reboot_actions = db.query(Action).filter(Action.state == "rebooted").all()
            for action in reboot_actions:
                load_reboot_state(action)
            # Commit the DB change due to rebooted nodes
            db.commit()
            # Delete the actions in final states
            final_actions = db.query(Action).filter(Action.state.in_(final_states)).all()
            for action in final_actions:
                logging.info("[%s] action is completed (current state: '%s')" % (
                    action.node_name, action.state))
                # Update the node status with the action state
                node = db.query(Node).filter(Node.name == action.node_name).first()
                # As the Node.name is the primary key, only one node should be selected
                if node is not None:
                    node.status = action.state
                    if action.state == "lost":
                        node.lost_state = action.reboot_state
                    if action.state == "destroyed":
                        # Delete action properties
                        properties = db.query(ActionProperty).filter(ActionProperty.node_name == node.name).all()
                        for prop in properties:
                            db.delete(prop)
                        # Update the node fields
                        free_reserved_node(node)
                # Delete the action
                db.delete(action)
            # Start actions for the recently configured nodes
            pending_nodes = db.query(Node
                ).filter(Node.status == "ready"
                ).all()
            for node in pending_nodes:
                logging.info("[%s] starts the deploy process" % node.name)
                act = new_action(node, db)
                init_action_process(act, "deploy")
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
                    # Execute the function associated to the action state
                    action_ret = False
                    try:
                        action_ret = getattr(exec_action_mod, state_fct)(action, db)
                    except:
                        logging.exception("[%s]" % action.node_name)
                        sys.exit(42)
                    if action_ret:
                        logging.info("[%s] successfully executes '%s'" % (action.node_name, state_fct))
                        # Update the state of the action
                        if state_fct.endswith("_exec") and STATE_DESC[state]["post"]:
                            # Execute the '_post' function
                            action.state = state_fct.replace("_exec", "_post")
                        else:
                            # Move to the next state of the process
                            next_state_move(action)
                    else:
                        # The node is not ready, test the reboot timeout
                        logging.warning("[%s] fails to execute '%s'" % (action.node_name, state_fct))
                        if action.updated_at is None:
                            action.updated_at = datetime.now().strftime(DATE_FORMAT)
                        updated = datetime.strptime(str(action.updated_at), DATE_FORMAT)
                        elapsedTime = (datetime.now() - updated).total_seconds()
                        action_state = action.state.replace("_exec", "").replace("_post","")
                        reboot_timeout = STATE_DESC[action_state]["before_reboot"]
                        do_lost = True
                        reboot_str = "%s?!%d" % (action.process, action.state_idx)
                        if reboot_timeout > 0 and \
                            action.process != "reboot" and action.reboot_state != reboot_str:
                            do_lost = False
                            if elapsedTime > reboot_timeout:
                                logging.warning("[%s] hard reboot the node" % action.node_name)
                                # Remember the last state of the current process
                                action.reboot_state = reboot_str
                                init_action_process(action, "reboot")
                            else:
                                logging.info("[%s] not ready since %d seconds" % (action.node_name, elapsedTime))
                        # The node is not ready, test the lost timeout
                        lost_timeout = STATE_DESC[action_state]["lost"]
                        if do_lost and lost_timeout > 0:
                            if elapsedTime > lost_timeout:
                                logging.warning("[%s] is lost. Stop monitoring it!" % action.node_name)
                                if action.process != "reboot":
                                    # Remember the last state of the current process
                                    action.reboot_state = reboot_str
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
