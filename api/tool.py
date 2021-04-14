from database.connector import close_session, open_session
from database.tables import Environment
from lib.config_loader import get_config
from sqlalchemy import distinct

# Remove unwanted charaters from the dangerous string
def safe_string(dangerous_str):
    return dangerous_str.translate({ord(c): "" for c in "\"!@#$%^&*()[]{};:,/<>?\|`~=+"})

def load_environment_names():
    db = open_session()
    env_names = [name[0] for name in db.query(distinct(Environment.name)).all()]
    close_session(db)
    config = get_config()
    config["configure_prop"][config["node_type"]]["environment"] = { "values": env_names, "mandatory": True }
