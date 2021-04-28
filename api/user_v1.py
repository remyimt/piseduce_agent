from api.auth import auth
from api.tool import safe_string
from database.connector import open_session, close_session, row2props
from database.tables import Action, ActionProperty, Environment, Node, NodeProperty, Switch
from datetime import datetime
from flask import Blueprint
from lib.config_loader import DATE_FORMAT, get_config
from importlib import import_module
from sqlalchemy import inspect, and_, or_
from agent_exec import free_reserved_node, new_action, init_action_process
import flask, json, logging


user_v1 = Blueprint("user_v1", __name__)


def row2dict(alchemyResult):
    return { c.key: getattr(alchemyResult, c.key) for c in inspect(alchemyResult).mapper.column_attrs }


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


# Get the list of the nodes
@user_v1.route("/node/list", methods=["POST"])
@auth
def node_list():
    db = open_session()
    result = {}
    if "properties" in flask.request.json and len(flask.request.json["properties"]) > 0:
        # Get the nodes with properties that match the request
        props = flask.request.json["properties"]
        if "name" in props:
            node = db.query(Node).filter(Node.name == props["name"]).first()
            if node is not None:
                result[node.name] = props
        else:
            # Build the AND filters
            ands = None
            for prop, value in props.items():
                if ands is None:
                    ands = and_(NodeProperty.prop_name == prop, NodeProperty.prop_value == value)
                else:
                    ands = ands | and_(NodeProperty.prop_name == prop, NodeProperty.prop_value == value)
            # Get the nodes with the right properties
            for node_prop in db.query(NodeProperty).filter(ands).all():
                if node_prop.name not in result:
                    result[node_prop.name] = {}
                result[node_prop.name][node_prop.prop_name] = node_prop.prop_value
    else:
        # Get all nodes
        nodes = db.query(Node).all()
        for n in nodes:
            result[n.name] = row2dict(n)
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes
@user_v1.route("/node/status", methods=["POST"])
@auth
def node_status():
    result = { "nodes": {} }
    # Get the status of the nodes
    db = open_session()
    nodes = []
    if "nodes" in flask.request.json:
        nodes = db.query(Node).filter(Node.name.in_(flask.request.json["nodes"])).all()
    elif "user" in flask.request.json:
        nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(Node.status != "configuring"
            ).all()
    for n in nodes:
        result["nodes"][n.name] = row2dict(n)
        if n.status == "in_progress":
            action = db.query(Action.state).filter(Action.node_name == n.name).first()
            if action is None or len(action.state) == 0:
                result["nodes"][n.name]["status"] = "in_progress"
            else:
                result["nodes"][n.name]["status"] = action.state.replace("_post", "").replace("_exec", "")
    os_passwords = db.query(ActionProperty).filter(ActionProperty.node_name.in_(result["nodes"].keys())
            ).filter(ActionProperty.prop_name.in_(["os_password", "percent"])).all()
    for pwd in os_passwords:
        result["nodes"][pwd.node_name][pwd.prop_name] = pwd.prop_value
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes with their properties
@user_v1.route("/node/prop", methods=["POST"])
@auth
def node_prop():
    db = open_session()
    # Get the nodes
    result = {}
    for n in db.query(Node).all():
        result[n.name] = row2dict(n)
    # Get the node properties
    for p in db.query(NodeProperty).all():
        if p.name in result:
            result[p.name][p.prop_name] = p.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/node/mine", methods=["POST"])
@auth
def my_node():
    result = {"states": [], "nodes": {}}
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    # Get the list of the states for the 'deploy' process
    py_module = import_module("%s.states" % get_config()["node_type"])
    PROCESS = getattr(py_module, "PROCESS")
    for p in PROCESS["deploy"]:
        if len(p["states"]) > len(result["states"]):
            result["states"] = p["states"]
    db = open_session()
    # Get my nodes
    node_names = []
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(Node.status != "configuring"
            ).all()
    for n in nodes:
        result["nodes"][n.name] = row2dict(n)
        node_names.append(n.name)
    props = db.query(NodeProperty).filter(NodeProperty.name.in_(result["nodes"].keys())).all()
    for p in props:
        result["nodes"][p.name][p.prop_name] = p.prop_value
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
    result = {}
    db = open_session()
    nodes = db.query(Node
        ).filter(Node.name.in_(wanted)
        ).filter(Node.status == "available"
        ).all()
    for n in nodes:
        n.status = "configuring"
        logging.info("[%s] change status to 'configuring'" % n.name)
        n.owner = user
        n.start_date = datetime.now().strftime(DATE_FORMAT)
        result[n.name] = row2dict(n)
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
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
        "duration": { "values": [], "mandatory": True }
    }
    # DB connection
    db = open_session()
    # List the existing environments
    envs = db.query(Environment).filter(Environment.prop_name == "web").all()
    for env in envs:
        conf_prop["environment"]["values"].append(env.name)
    # Get the nodes in the 'configuring' state
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(Node.status == "configuring"
            ).all()
    for n in nodes:
        if len(conf_prop) == 3:
            node_type = get_config()["node_type"]
            conf_prop.update(get_config()["configure_prop"][node_type])
        result[n.name] = conf_prop
    close_session(db)
    return json.dumps(result)


@user_v1.route("/deploy", methods=["POST"])
@auth
def deploy():
    # Check the parameters
    error_msg = { "parameters": 
            "user: 'email@is.fr', 'nodes': {'node-3': { 'node_bin': 'my_bin', 'duration': '4', 'environment': 'my-env' }}" }
    if "user" not in flask.request.json or "@" not in flask.request.json["user"] or \
        "nodes" not in flask.request.json:
        return json.dumps(error_msg)
    # Check the nodes dictionnary
    node_prop = flask.request.json["nodes"]
    if isinstance(node_prop, dict):
        for val in node_prop.values():
            if not isinstance(val, dict):
                return json.dumps(error_msg)
    else:
        return json.dumps(error_msg)
    # Get the list of properties for the configuration
    node_type = get_config()["node_type"]
    conf_prop = get_config()["configure_prop"][node_type]
    # Get the node with the 'configuring' status
    result = {}
    db = open_session()
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(or_(Node.status == "configuring", Node.status == "ready")
            ).all()
    for n in nodes:
        if n.name in node_prop:
            duration = int(node_prop[n.name].pop("duration"))
            # Remove special characters from the node bin name
            safe_value = safe_string(node_prop[n.name].pop("node_bin"))
            # Remove spaces from value
            safe_value = safe_value.replace(" ", "_")
            node_bin = safe_value
            result[n.name] = {}
            # Check required properties
            required = [ prop for prop in conf_prop if conf_prop[prop]["mandatory"] ]
            for prop in required:
                if prop not in node_prop[n.name]:
                    if "missing" not in result[n.name]:
                        result[n.name]["missing"] = [ prop ]
                    else:
                        result[n.name]["missing"].append(prop)
            if len(result[n.name]) == 0:
                # Delete the existing configuration for this node
                existing = db.query(ActionProperty).filter(ActionProperty.node_name == n.name).all()
                for to_del in existing:
                    db.delete(to_del)
                # Write the configuration to the database
                for prop in node_prop[n.name]:
                    if len(node_prop[n.name][prop]) > 0:
                        act_prop = ActionProperty()
                        act_prop.node_name = n.name
                        act_prop.prop_name = prop
                        if "ssh_key" in prop:
                            act_prop.prop_value = node_prop[n.name][prop]
                        else:
                            # Remove special characters from value
                            safe_value = safe_string(node_prop[n.name][prop])
                            # Remove spaces from value
                            safe_value = safe_value.replace(" ", "_")
                            act_prop.prop_value = safe_value
                        db.add(act_prop)
                n.status = "ready"
                n.bin = node_bin
                n.duration = duration
                logging.info("[%s] change status to 'ready'" % n.name)
                result[n.name]["status"] = n.status
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
    # Get information about the requested nodes
    result = {}
    db = open_session()
    nodes = db.query(Node
            ).filter(Node.name.in_(wanted)
            ).filter(Node.owner == user
            ).all()
    for n in nodes:
        # Delete actions in progress
        actions = db.query(Action).filter(Action.node_name == n.name).all()
        for action in actions:
            db.delete(action)
        if n.status == "configuring":
            free_reserved_node(n)
        else:
            # Create a new action to start the destroy action
            node_action = new_action(n, db)
            init_action_process(node_action, "destroy")
            db.add(node_action)
        result[n.name] = "success"
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
    nodes = db.query(Node
            ).filter(Node.name.in_(wanted)
            ).filter(Node.owner == user
            ).all()
    for n in nodes:
        node_action = db.query(Action).filter(Action.node_name == n.name).first()
        if node_action is None:
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
        # The deployment is completed, add a new action
        init_action_process(node_action, "reboot")
        db.add(node_action)
        result[n.name] = "success"
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
    nodes = db.query(Node
            ).filter(Node.name.in_(wanted)
            ).filter(Node.owner == user
            ).all()
    for n in nodes:
        node_action = db.query(Action).filter(Action.node_name == n.name).first()
        if node_action is None:
            # The deployment is completed, add a new action
            node_action = new_action(n, db)
        # The deployment is completed, add a new action
        init_action_process(node_action, "deploy")
        db.add(node_action)
        result[n.name] = "success"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)
