from api.admin_v1 import admin_v1
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

if __name__ == '__main__':
    logging.basicConfig(filename='info_api.log', level=logging.INFO)
    load_config()
    create_tables()
    worker_api.run()
