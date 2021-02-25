from api.auth import auth
from database.connector import open_session, close_session
from database.tables import Environment, Node, NodeProperty, Switch 
from lib.config_loader import load_config
import flask, json


admin_v1 = flask.Blueprint("admin_v1", __name__)


@admin_v1.route("/add/switch", methods=["POST"])
@auth
def add_switch():
    switch_data = flask.request.json
    switch_props = load_config()["switch_prop"]
    # Check if all properties belong to the POST data
    no_data = [key_data for key_data in switch_props if key_data not in switch_data.keys()]
    if len(no_data) == 0:
        db = open_session()
        existing = db.query(Switch).filter_by(name = switch_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        for prop in switch_props:
            if prop != "name":
                switch_db = Switch()
                switch_db.name = switch_data["name"]
                switch_db.prop_name = prop
                switch_db.prop_value = switch_data[prop]
                db.add(switch_db)
        close_session(db)
        return json.dumps({ "switch": switch_data["name"] })
    else:
        return json.dumps({"missing": no_data })


@admin_v1.route("/add/node", methods=["POST"])
@auth
def add_node():
    node_data = flask.request.json
    node_props = load_config()["node_prop"].copy()
    worker_type = load_config()["action_driver"]
    node_props += load_config()[worker_type + "_prop"]
    # Check if all properties belong to the POST data
    no_data = [key_data for key_data in node_props if key_data not in node_data.keys()]
    if len(no_data) == 0:
        db = open_session()
        existing = db.query(Node).filter_by(name = node_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        existing = db.query(NodeProperty).filter_by(name = node_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        node_db = Node()
        node_db.type = worker_type
        node_db.name = node_data["name"]
        node_db.ip = node_data["ip"]
        node_db.status = "available"
        node_db.owner = None
        db.add(node_db)
        for prop in node_props:
            if prop != "name":
                prop_db = NodeProperty()
                prop_db.name = node_data["name"]
                prop_db.prop_name = prop
                prop_db.prop_value = node_data[prop]
                db.add(prop_db)
        close_session(db)
        return { "node": node_data["name"] }
    else:
        return {"missing": no_data }


@admin_v1.route("/add/environment", methods=["POST"])
@auth
def add_environment():
    env_data = flask.request.json
    env_props = load_config()["env_prop"]
    # Check if all properties belong to the POST data
    no_data = [key_data for key_data in env_props if key_data not in env_data.keys()]
    if len(no_data) == 0:
        db = open_session()
        existing = db.query(Environment).filter_by(name = env_data["name"]).all()
        for to_del in existing:
            db.delete(to_del)
        for prop in env_props:
            if prop != "name":
                if prop == "desc":
                    for d in env_data["desc"]:
                        env_db = Environment()
                        env_db.name = env_data["name"]
                        env_db.prop_name = prop
                        env_db.prop_value = d
                        db.add(env_db)
                else:
                    env_db = Environment()
                    env_db.name = env_data["name"]
                    env_db.prop_name = prop
                    env_db.prop_value = env_data[prop]
                    db.add(env_db)
        close_session(db)
        return { "environment": env_data["name"] }
    else:
        return {"missing": no_data }


@admin_v1.route("/delete/<el_type>", methods=["POST"])
@auth
def delete(el_type):
    data = flask.request.json
    props = [ "name" ]
    # Check if all properties belong to the POST data
    no_data = [key_data for key_data in props if key_data not in data.keys()]
    if len(no_data) == 0:
        if el_type == "node":
            db = open_session()
            existing = db.query(Node).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            existing = db.query(NodeProperty).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "switch":
            db = open_session()
            existing = db.query(Switch).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        elif el_type == "environment":
            db = open_session()
            existing = db.query(Environment).filter_by(name = data["name"]).all()
            for to_del in existing:
                db.delete(to_del)
            close_session(db)
        else:
            return {"type_error": data["type"] }
        return { "delete": len(existing) }
    else:
        return {"missing": no_data }


