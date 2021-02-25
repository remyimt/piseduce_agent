from api.auth import auth
from database.connector import open_session, close_session, row2dict
from database.tables import ActionProperty, Environment, Node, NodeProperty, Switch
from datetime import datetime
from flask import Blueprint
from lib.config_loader import DATE_FORMAT, load_config
from sqlalchemy import or_
import flask, json


user_v1 = Blueprint("user_v1", __name__)


@user_v1.route("/switch", methods=["POST"])
@auth
def list_switch():
    db = open_session()
    # Get the nodes
    result = {}
    switches = db.query(Switch).all()
    for s in switches:
        if s.name not in result:
            result[s.name] = {}
        result[s.name][s.prop_name] = s.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/environment", methods=["POST"])
@auth
def list_environment():
    db = open_session()
    # Get the nodes
    result = {}
    envs = db.query(Environment).all()
    for e in envs:
        if e.name not in result:
            result[e.name] = {}
        result[e.name][e.prop_name] = e.prop_value
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes
@user_v1.route("/node", methods=["POST"])
@auth
def list_node():
    db = open_session()
    # Get the nodes
    result = {}
    nodes = db.query(Node).all()
    for n in nodes:
        result[n.name] = row2dict(n)
    close_session(db)
    return json.dumps(result)


# Get the list of the nodes with their properties
@user_v1.route("/node-prop", methods=["POST"])
def list_node_prop():
    db = open_session()
    # Get the nodes
    result = {}
    nodes = db.query(Node).all()
    for n in nodes:
        result[n.name] = row2dict(n)
    # Get the node properties
    props = db.query(NodeProperty).all()
    for p in props:
        result[p.name][p.prop_name] = p.prop_value
    close_session(db)
    return json.dumps(result)


@user_v1.route("/my-node", methods=["POST"])
@auth
def my_node():
    if "user" not in flask.request.json or "@" not in flask.request.json["user"]:
        return json.dumps({ "parameters": "user: 'email@is.fr'" })
    db = open_session()
    # Get my nodes
    result = {}
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).all()
    for n in nodes:
        result[n.name] = n.status
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
    conf_prop = load_config()["configure_prop"]
    db = open_session()
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(Node.status == "configuring"
            ).all()
    for n in nodes:
        result[n.name] = conf_prop[n.type]
    close_session(db)
    return json.dumps(result)


@user_v1.route("/deploy", methods=["POST"])
@auth
def deploy():
    # Check the parameters
    error_msg = { "parameters": 
            "user: 'email@is.fr', 'nodes': {'node-3': { 'name': 'my_deployment', 'duration': '4' }}" }
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
    conf_prop = load_config()["configure_prop"]
    # Get the node with the 'configuring' status
    result = {}
    db = open_session()
    nodes = db.query(Node
            ).filter(Node.owner == flask.request.json["user"]
            ).filter(or_(Node.status == "configuring", Node.status == "ready")
            ).all()
    for n in nodes:
        if n.name in node_prop:
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
                existing = db.query(ActionProperty).filter_by(node_name = n.name).all()
                for to_del in existing:
                    db.delete(to_del)
                # Write the configuration to the database
                for prop in node_prop[n.name]:
                    act_prop = ActionProperty()
                    act_prop.node_name = n.name
                    act_prop.prop_name = prop
                    act_prop.prop_value = node_prop[n.name][prop]
                    db.add(act_prop)
                n.status = "ready"
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
            ).filter(Node.status == "used"
            ).filter(Node.owner == user
            ).all()
    for n in nodes:
        n.status = "available"
        n.owner = None
        n.start_date = None
        result[n.name] = row2dict(n)
    close_session(db)
    # Build the result
    for n in wanted:
        if n not in result:
            result[n] = "failure"
    return json.dumps(result)
