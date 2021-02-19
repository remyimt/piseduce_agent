from api.auth import auth
from flask import Blueprint


debug_v1 = Blueprint("debug_v1", __name__)


@debug_v1.route("/status")
def status():
    return {"status": "running" }


@debug_v1.route("/auth", methods=["POST"])
@auth
def auth():
    return {"auth": "success" }
