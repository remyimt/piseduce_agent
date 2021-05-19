from api.auth import auth
from api.tool import safe_string
from database.connector import open_session, close_session, row2props
from database.tables import Action, ActionProperty, Environment, Node, Schedule, Switch
from datetime import datetime, timedelta, timezone
from flask import Blueprint
from lib.config_loader import DATE_FORMAT, get_config
from importlib import import_module
from sqlalchemy import inspect, and_, or_
from agent_exec import free_reserved_node, new_action, init_action_process, save_reboot_state
import flask, json, logging


user_v1 = Blueprint("user_v1", __name__)


def row2dict(alchemyResult):
    result = {}
    for c in inspect(alchemyResult).mapper.column_attrs:
        if isinstance(getattr(alchemyResult, c.key), datetime):
            result[c.key] = getattr(alchemyResult, c.key).strftime(DATE_FORMAT)
        else:
            result[c.key] = getattr(alchemyResult, c.key)
    return result


# List the DHCP clients
@user_v1.route("/client/list", methods=["POST"])
@auth
def list_dhcp():
    result = {}
    with open("/etc/dnsmasq.conf", "r") as dhcp_conf:
        for line in dhcp_conf.readlines():
            if line.startswith("dhcp-host="):
                line = line.replace(" ", "")
                line = line[10:]
                dhcp_info = line.split(",")
                result[dhcp_info[1]] = { "mac_address": dhcp_info[0], "ip": dhcp_info[2] }
    return json.dumps(result)


@user_v1.route("/switch/list", methods=["POST"])
@auth
def list_switch():
    db = open_session()
    # Get the switches
    result = {}
    switches = db.query(Switch).all()
    for s in switches:
        if s.name not in result:
            result[s.name] = {}
        result[s.name][s.prop_name] = s.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/environment/list", methods=["POST"])
@auth
def list_environment():
    db = open_session()
    # Get the environments
    result = {}
    envs = db.query(Environment).all()
    for e in envs:
        if e.name not in result:
            result[e.name] = {}
        result[e.name][e.prop_name] = e.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/node/state", methods=["POST"])
@auth
def node_state():
    result = { "nodes": {} }
    db = open_session()
    nodes = []
    if "nodes" in flask.request.json:
        nodes = db.query(Schedule).filter(Schedule.node_name.in_(flask.request.json["nodes"])).all()
    elif "user" in flask.request.json:
        nodes = db.query(Schedule
            ).filter(Schedule.owner == flask.request.json["user"]
            ).filter(Schedule.state != "configuring"
            ).all()
    # Get the state of the nodes
    for n in nodes:
        result["nodes"][n.node_name] = { "name": n.node_name, "state": n.state, "bin": n.bin }
        if n.state == "in_progress":
            # An action is in progress, get the state of this action
            action = db.query(Action.state).filter(Action.node_name == n.node_name).first()
            if action is None or action.state is None or len(action.state) == 0:
                result["nodes"][n.node_name]["state"] = n.state
            else:
                result["nodes"][n.node_name]["state"] = action.state.replace("_post", "").replace("_exec", "")
        if n.state == "ready":
            # There is no action associated to this node
            if n.action_state is not None and len(n.action_state) > 0:
                result["nodes"][n.node_name]["state"] = n.action_state
    # Get both the OS password and the environment copy progress of the nodes
    action_props = db.query(ActionProperty
            ).filter(ActionProperty.node_name.in_(result["nodes"].keys())
            ).filter(ActionProperty.prop_name.in_(["os_password", "percent"])).all()
    for prop in action_props:
        result["nodes"][prop.node_name][prop.prop_name] = prop.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/node/schedule", methods=["POST"])
@auth
def node_schedule():
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    result = { "nodes": {} }
    db = open_session()
    for sch in db.query(Schedule).all():
        if sch.node_name not in result["nodes"]:
            result["nodes"][sch.node_name] = {}
        """
        if sch.node_name not in result["nodes"]:
            result["nodes"][sch.node_name] = {
                "owner": sch.owner,
                "start_hour": str(sch.start_date).split()[1],
                "end_hour": str(sch.end_date).split()[1]
            }
        """
        hours_added = 0
        delta = timedelta(hours = hours_added)
        while sch.start_date + delta < sch.end_date:
            new_date = sch.start_date + delta
            day_str = str(new_date).split()[0]
            hour_str = str(new_date.hour)
            if day_str not in result["nodes"][sch.node_name]:
                result["nodes"][sch.node_name][day_str] = {}
            if sch.owner not in result["nodes"][sch.node_name][day_str]:
                result["nodes"][sch.node_name][day_str][sch.owner] = {
                    "hours": [],
                    "owner": sch.owner,
                    "start_hour": str(sch.start_date).split()[1],
                    "end_hour": str(sch.end_date).split()[1]
                }
            result["nodes"][sch.node_name][day_str][sch.owner]["hours"].append(hour_str)
            hours_added += 1
            delta = timedelta(hours = hours_added)
        day_str = str(sch.end_date).split()[0]
        hour_str = str(sch.end_date.hour)
        if day_str not in result["nodes"][sch.node_name]:
            result["nodes"][sch.node_name][day_str] = []
        if sch.owner not in result["nodes"][sch.node_name][day_str]:
            result["nodes"][sch.node_name][day_str][sch.owner] = {
                "hours": [],
                "owner": sch.owner,
                "start_hour": str(sch.start_date).split()[1],
                "end_hour": str(sch.end_date).split()[1]
            }
        result["nodes"][sch.node_name][day_str][sch.owner]["hours"].append(hour_str)
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes with their properties
@user_v1.route("/node/list", methods=["POST"])
@auth
def node_prop():
    result = {}
    db = open_session()
    # Get the node properties
    for p in db.query(Node).filter(Node.node_name != "pimaster").all():
        if p.node_name not in result:
            result[p.node_name] = {}
        result[p.node_name][p.prop_name] = p.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/node/mine", methods=["POST"])
@auth
def my_node():
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    result = {"states": [], "nodes": {}}
    # Get the list of the states for the 'deploy' process
    py_module = import_module("%s.states" % get_config()["node_type"])
    PROCESS = getattr(py_module, "PROCESS")
    for p in PROCESS["deploy"]:
        if len(p["states"]) > len(result["states"]):
            result["states"] = p["states"]
    db = open_session()
    # Get my nodes
    node_names = []
    nodes = db.query(Schedule
            ).filter(Schedule.owner == flask.request.json["user"]
            ).filter(Schedule.state != "configuring"
            ).all()
    for n in nodes:
        result["nodes"][n.node_name] = row2dict(n)
        node_names.append(n.node_name)
    props = db.query(Node).filter(Node.node_name.in_(result["nodes"].keys())).all()
    for p in props:
        result["nodes"][p.node_name][p.prop_name] = p.prop_value
    envs = db.query(ActionProperty
        ).filter(ActionProperty.node_name.in_(result["nodes"].keys())
        ).filter(ActionProperty.prop_name.in_(["environment", "os_password"])
        ).all()
    env_web = {}
    for e in envs:
        if e.prop_name == "environment":
            # Check if the environment provides a web interface
            if e.prop_value not in env_web:
                has_web = db.query(Environment).filter(Environment.name == e.prop_value
                    ).filter(Environment.prop_name == "web").first().prop_value
                env_web[e.prop_value] = has_web
            if env_web[e.prop_value] == "true":
                #result["nodes"][e.node_name]["url"] = "http://%s:8181" % result["nodes"][e.node_name]["ip"]
                # Hack for the PiSeduce cluster
                result["nodes"][e.node_name]["url"] = "https://pi%02d.seduce.fr" % (
                        int(result["nodes"][e.node_name]["port_number"]))
        result["nodes"][e.node_name][e.prop_name] = e.prop_value
    close_session(db)
    return json.dumps(result)


## Actions
@user_v1.route("/reserve", methods=["POST"])
@auth
def reserve():
    post_data = flask.request.json
    # Check POST data
    if "filter" not in post_data or "user" not in post_data or \
        "start_date" not in post_data or "duration" not in post_data:
            logging.error("Missing parameters: '%s'" % post_data)
            return json.dumps({
                "parameters": "filter: {...}, user: 'email@is.fr', start_date: '2021-06-21 14:36:32', 'duration': 3" })
    if len(post_data["start_date"]) != 19:
        logging.error("Wrong date format: '%s'" % post_data["start_date"])
        return json.dumps({"parameters": "Wrong date format (required: YYYY-MM-DD HH-MM)"})
    result = { "nodes": [] }
    user = post_data["user"]
    f = post_data["filter"]
    # f = {'nb_nodes': '3', 'model': 'RPI4B8G', 'switch': 'main_switch'}
    nb_nodes = int(f["nb_nodes"])
    del f["nb_nodes"]
    start_date = datetime.strptime(post_data["start_date"], DATE_FORMAT)
    hours_added = timedelta(hours = post_data["duration"])
    end_date = start_date + hours_added
    db = open_session()
    filtered_nodes = []
    if "name" in f:
        # Node names are unique identifiers
        node = db.query(Node).filter(Node.node_name == f["name"]).first()
        if node is not None:
            filtered_nodes.append(node.node_name)
    else:
        # Get the node properties used in the filter
        node_props = {}
        if len(f) == 0:
            nodes = db.query(Node).all()
        else:
            nodes = db.query(Node).filter(Node.prop_name.in_(f.keys())).all()
        for prop in nodes:
            if prop.node_name not in node_props:
                node_props[prop.node_name] = {}
            node_props[prop.node_name][prop.prop_name] = prop.prop_value
        for node_name in node_props:
            ok_filtered = True
            for prop in f:
                if node_props[node_name][prop] != f[prop]:
                    ok_filtered = False
            if ok_filtered:
                filtered_nodes.append(node_name)
    # Check the availability of the filtered nodes
    logging.warning("Filtered nodes: %s" % filtered_nodes)
    selected_nodes = []
    for node_name in filtered_nodes:
        ok_selected = True
        # Move the start date back 15 minutes to give the time for destroying the previous reservation
        minutes_removed = timedelta(minutes = 15)
        back_date = start_date - minutes_removed
        # Check the schedule of the existing reservations
        for reservation in db.query(Schedule).filter(Schedule.node_name == node_name).all():
            # Only one reservation for a specific node per user
            if reservation.owner == user:
                ok_selected = False
            # There is a reservation at the same date
            if (back_date > reservation.start_date and back_date < reservation.end_date) or (
                back_date < reservation.start_date and end_date > reservation.start_date):
                ok_selected = False
        if ok_selected:
            # Add the node to the reservation
            selected_nodes.append(node_name)
            if len(selected_nodes) == nb_nodes:
                # Exit when the required number of nodes is reached
                break;
    logging.warning("Selected nodes: %s" % selected_nodes)
    # Reserve the nodes
    for node_name in selected_nodes:
        res = Schedule()
        res.node_name = node_name
        res.owner = user
        res.start_date = start_date
        res.end_date = end_date
        res.state = "configuring"
        res.action_state = ""
        db.add(res)
    close_session(db)
    result["nodes"] = selected_nodes
    return json.dumps(result)


@user_v1.route("/configure", methods=["POST"])
@auth
def configure():
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    result = {}
    # Common properties to every kind of nodes
    conf_prop = {
        "node_bin": { "values": [], "mandatory": True },
        "environment": { "values": [], "mandatory": True },
    }
    # DB connection
    db = open_session()
    # List the existing environments
    envs = db.query(Environment).filter(Environment.prop_name == "web").all()
    for env in envs:
        conf_prop["environment"]["values"].append(env.name)
    # Get the nodes in the 'configuring' state
    nodes = db.query(Schedule
            ).filter(Schedule.owner == flask.request.json["user"]
            ).filter(Schedule.state == "configuring"
            ).all()
    for n in nodes:
        if len(conf_prop) == 2:
            node_type = get_config()["node_type"]
            conf_prop.update(get_config()["configure_prop"][node_type])
        result[n.node_name] = conf_prop.copy()
        result[n.node_name]["start_date"] = n.start_date.strftime(DATE_FORMAT)
        result[n.node_name]["end_date"] = n.end_date.strftime(DATE_FORMAT)
    close_session(db)
    return json.dumps(result)


@user_v1.route("/deploy", methods=["POST"])
@auth
def deploy():
    # Check the parameters
    error_msg = { "parameters": 
            "user: 'email@is.fr', 'nodes': {'node-3': { 'node_bin': 'my_bin', 'environment': 'my-env' }}" }
    if "user" not in flask.request.json or "@" not in flask.request.json["user"] or \
        "nodes" not in flask.request.json:
        return json.dumps(error_msg)
    # Check the nodes dictionnary
    node_prop = flask.request.json["nodes"]
    user_email = flask.request.json["user"]
    if isinstance(node_prop, dict):
        for val in node_prop.values():
            if not isinstance(val, dict):
                return json.dumps(error_msg)
    else:
        return json.dumps(error_msg)
    # Get the list of properties for the configuration
    node_type = get_config()["node_type"]
    conf_prop = get_config()["configure_prop"][node_type]
    # Get the node with the 'configuring' state
    result = {}
    db = open_session()
    # Search the nodes to deploy in the schedule table (nodes in 'configuring' state)
    nodes = db.query(Schedule
            ).filter(Schedule.owner == user_email
            ).filter(Schedule.state == "configuring"
            ).all()
    for n in nodes:
        if n.node_name in node_prop:
            # Remove special characters from the node bin name
            safe_value = safe_string(node_prop[n.node_name].pop("node_bin"))
            # Remove spaces from value
            safe_value = safe_value.replace(" ", "_")
            node_bin = safe_value
            result[n.node_name] = {}
            # Check required properties
            required = [ prop for prop in conf_prop if conf_prop[prop]["mandatory"] ]
            for prop in required:
                if prop not in node_prop[n.node_name]:
                    if "missing" not in result[n.node_name]:
                        result[n.node_name]["missing"] = [ prop ]
                    else:
                        result[n.node_name]["missing"].append(prop)
            if len(result[n.node_name]) == 0:
                # Delete the existing configuration for this node
                existing = db.query(ActionProperty).filter(ActionProperty.node_name == n.node_name).all()
                for to_del in existing:
                    db.delete(to_del)
                # Write the configuration to the database
                for prop in node_prop[n.node_name]:
                    if len(node_prop[n.node_name][prop]) > 0:
                        act_prop = ActionProperty()
                        act_prop.node_name = n.node_name
                        act_prop.prop_name = prop
                        act_prop.owner = user_email
                        if "ssh_key" in prop:
                            act_prop.prop_value = node_prop[n.node_name][prop]
                        else:
                            # Remove special characters from value
                            safe_value = safe_string(node_prop[n.node_name][prop])
                            # Remove spaces from value
                            safe_value = safe_value.replace(" ", "_")
                            act_prop.prop_value = safe_value
                        db.add(act_prop)
                n.state = "ready"
                n.bin = node_bin
                logging.info("[%s] change state to 'ready'" % n.node_name)
                result[n.node_name]["state"] = n.state
    close_session(db)
    return json.dumps(result)


@user_v1.route("/destroy", methods=["POST"])
@auth
def destroy():
    # Check POST data
    if "nodes" not in flask.request.json or \
        "user" not in flask.request.json:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = flask.request.json["nodes"]
    user = flask.request.json["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    logging.info("Destroying the nodes: %s" % wanted)
    result = {}
    db = open_session()
    # Delete actions in progress for the nodes to destroy
    actions = db.query(Action).filter(Action.node_name.in_(wanted)).all()
    for action in actions:
        db.delete(action)
    # Get the reservations to destroy
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "configuring":
            # The node is not deployed, delete the reservation and the associated properties
            free_reserved_node(db, n.node_name)
            result[n.node_name] = "success"
        else:
            # Create a new action to start the destroy action
            node_action = new_action(n, db)
            init_action_process(node_action, "destroy")
            db.add(node_action)
            result[n.node_name] = "success"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


@user_v1.route("/hardreboot", methods=["POST"])
@auth
def hardreboot():
    result = {}
    # Check POST data
    if "nodes" not in flask.request.json or \
        "user" not in flask.request.json:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = flask.request.json["nodes"]
    user = flask.request.json["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "ready":
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
            save_reboot_state(node_action, db)
            init_action_process(node_action, "reboot")
            db.add(node_action)
            result[n.node_name] = "success"
        else:
            logging.error("[%s] can not reboot because the state is not 'ready' (state: %s)" % (
                n.node_name, n.state))
            result[n.node_name] = "failure: %s is not ready" % n.node_name
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


@user_v1.route("/deployagain", methods=["POST"])
@auth
def deployagain():
    result = {}
    # Check POST data
    if "nodes" not in flask.request.json or \
        "user" not in flask.request.json:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = flask.request.json["nodes"]
    user = flask.request.json["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if n.state == "ready":
            node_action = db.query(Action).filter(Action.node_name == n.node_name).first()
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
            # The deployment is completed, add a new action
            init_action_process(node_action, "deploy")
            db.add(node_action)
            result[n.node_name] = "success"
        else:
            result[n.node_name] = "failure: %s is not ready" % n.node_name
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)


# Increase the duration of existing reservations
@user_v1.route("/extend", methods=["POST"])
@auth
def extend():
    result = {}
    # Check POST data
    if "nodes" not in flask.request.json or \
        "user" not in flask.request.json:
        return json.dumps({ "parameters": "nodes: ['name1', 'name2' ], user: 'email@is.fr'" })
    wanted = flask.request.json["nodes"]
    user = flask.request.json["user"]
    if len(user) == 0 or '@' not in  user:
        for n in wanted:
            result[n] = "no_email"
        return json.dumps(result)
    # Get the current date
    now = datetime.now(timezone.utc)
    now = now.replace(tzinfo = None)
    # Get information about the requested nodes
    db = open_session()
    nodes = db.query(Schedule
            ).filter(Schedule.node_name.in_(wanted)
            ).filter(Schedule.owner == user
            ).all()
    for n in nodes:
        if (n.end_date - now).total_seconds() < 3600:
            if (n.end_date - n.start_date).days > 4:
                result[n.node_name] = "failure: the maximum duration of the reservation is reached"
            else:
                n.end_date += n.end_date - n.start_date
                result[n.node_name] = "success"
        else:
            result[n.node_name] = "failure: it is too early to extend the reservation"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)
