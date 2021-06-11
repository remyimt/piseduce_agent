# Load the configuration file
import sys
from lib.config_loader import load_config, get_config

if len(sys.argv) != 2:
    print("The configuration file is required in parameter.")
    print("For example, 'python3 %s config.json'" % sys.argv[0])
    sys.exit(2)
load_config(sys.argv[1])

from api.admin_v1 import admin_v1
from api.debug_v1 import debug_v1
from api.user_v1 import user_v1
from flask import Flask
from importlib import import_module
import logging, sys

# Create the application
agent_api = Flask(__name__)
# Add routes from blueprints
agent_api.register_blueprint(user_v1, url_prefix='/v1/user/')
agent_api.register_blueprint(admin_v1, url_prefix='/v1/admin/')
agent_api.register_blueprint(debug_v1, url_prefix='/v1/debug/')

if __name__ == '__main__':
    logging.basicConfig(filename='info_api.log', level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # Get the python module from the type of the nodes managed by this agent
    node_type = get_config()["node_type"]
    api_exec_mod = import_module("%s.api" % node_type)
    # Add the environment names from the database to the config
    getattr(api_exec_mod, "load_environments")()
    # Start the application
    port_number = get_config()["port_number"]
    agent_api.run(port=port_number, host="0.0.0.0")
