from api.auth import auth
from lib.config_loader import load_config
from flask import Blueprint
import json


debug_v1 = Blueprint("debug_v1", __name__)


@debug_v1.route("/status")
def status():
    return json.dumps({"status": "running", "type": load_config()["action_driver"] })


@debug_v1.route("/auth", methods=["POST"])
@auth
def auth():
    return json.dumps({"auth": "success" })
