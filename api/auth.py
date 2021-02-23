from lib.config_loader import load_config
import flask, functools

def auth(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if flask.request.json is None:
            flask.abort(503)
        else:
            token = flask.request.json.get("token")
            if len(token) != 10 and not token in load_config()["auth_token"]:
                flask.abort(503)
        return f(*args, **kwargs)
    return decorated_function
