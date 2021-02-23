from api.admin_v1 import admin_v1
from api.debug_v1 import debug_v1
from api.user_v1 import user_v1
from database.connector import create_tables
from flask import Flask
from lib.config_loader import load_config
import logging

# Create the application
worker_api = Flask(__name__)
# Add routes from blueprints
worker_api.register_blueprint(user_v1, url_prefix='/v1/user/')
worker_api.register_blueprint(admin_v1, url_prefix='/v1/admin/')
worker_api.register_blueprint(debug_v1, url_prefix='/v1/debug/')

if __name__ == '__main__':
    logging.basicConfig(filename='info_api.log', level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    port_number = load_config()["port_number"]
    worker_api.run(port=port_number, host="0.0.0.0")
