from api.auth import auth
from lib.config_loader import get_config
from flask import Blueprint
import json


debug_v1 = Blueprint("debug_v1", __name__)


@debug_v1.route("/status")
def status():
    return json.dumps({"status": "running", "type": get_config()["node_type"] })


@debug_v1.route("/auth", methods=["POST"])
@auth
def auth():
    return json.dumps({"auth": "success" })
