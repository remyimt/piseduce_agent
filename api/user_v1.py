from api.auth import auth
from database.connector import open_session, close_session, row2props
from database.tables import Action, ActionProperty, Environment, Node, NodeProperty, Switch
from datetime import datetime
from flask import Blueprint
from lib.config_loader import DATE_FORMAT, get_config
from sqlalchemy import inspect, and_, or_
import flask, json, logging


user_v1 = Blueprint("user_v1", __name__)


def row2dict(alchemyResult):
    return { c.key: getattr(alchemyResult, c.key) for c in inspect(alchemyResult).mapper.column_attrs }


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
        nodes = db.query(Node).filter(Node.type == get_config()["node_type"]).all()
        for n in nodes:
            result[n.name] = row2dict(n)
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes
@user_v1.route("/node/status", methods=["POST"])
@auth
def node_status():
    result = {}
    db = open_session()
    nodes = []
    if "nodes" in flask.request.json:
        nodes = db.query(Node).filter(Node.name.in_(flask.request.json["nodes"])).all()
    elif "user" in flask.request.json:
        nodes = db.query(Node
            ).filter(Node.type == get_config()["node_type"]
            ).filter(Node.owner == flask.request.json["user"]
            ).all()
    for n in nodes:
        result[n.name] = row2dict(n)
        if n.status == "in_progress":
            action = db.query(Action.state).filter(Action.node_name == n.name).first()
            if action is None or len(action.state) == 0:
                result[n.name]["status"] = "in_progress"
            else:
                result[n.name]["status"] = action.state.replace("_post", "").replace("_exec", "")
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes with their properties
@user_v1.route("/node/prop", methods=["POST"])
@auth
def node_prop():
    db = open_session()
    # Get the nodes
    result = {}
    for n in db.query(Node).filter(Node.type == get_config()["node_type"]).all():
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
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    db = open_session()
    # Get my nodes
    node_names = []
    result = {}
    nodes = db.query(Node
            ).filter(Node.type == get_config()["node_type"]
            ).filter(Node.owner == flask.request.json["user"]
            ).all()
    for n in nodes:
        result[n.name] = row2dict(n)
        node_names.append(n.name)
    props = db.query(NodeProperty).filter(NodeProperty.name.in_(result.keys())).all();
    for p in props:
        result[p.name][p.prop_name] = p.prop_value
    envs = db.query(ActionProperty
        ).filter(ActionProperty.node_name.in_(result.keys())
        ).filter(ActionProperty.prop_name == "environment"
        ).all();
    for e in envs:
        result[e.node_name][e.prop_name] = e.prop_value
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
        "duration": { "values": [], "mandatory": True }
    }
    # Get the specific properties according to the node type
    db = open_session()
    nodes = db.query(Node
            ).filter(Node.type == get_config()["node_type"]
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(Node.status == "configuring"
            ).all()
    for n in nodes:
        if len(conf_prop) == 2:
            conf_prop.update(get_config()["configure_prop"][n.type])
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
    conf_prop = get_config()["configure_prop"]
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
            # Remove special characters from value
            safe_value = node_prop[n.name].pop("node_bin").translate({ord(c): "" for c in "\"!@#$%^&*()[]{};:,./<>?\|`~=+"})
            # Remove spaces from value
            safe_value = safe_value.replace(" ", "_")
            node_bin = safe_value
            result[n.name] = {}
            # Check required properties
            required = [ prop for prop in conf_prop[n.type] if conf_prop[n.type][prop]["mandatory"] ]
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
                        # Remove special characters from value
                        safe_value = node_prop[n.name][prop].translate({ord(c): "" for c in "\"!@#$%^&*()[]{};:,./<>?\|`~=+"})
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
        n.status = "available"
        logging.info("[%s] change status to 'available'" % n.name)
        n.bin = None
        n.owner = None
        n.duration = None
        n.start_date = None
        actions = db.query(ActionProperty).filter(ActionProperty.node_name == n.name).all()
        for action in actions:
            db.delete(action)
        actions = db.query(Action).filter(ActionProperty.node_name == n.name).all()
        for action in actions:
            db.delete(action)
        result[n.name] = "success"
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)
