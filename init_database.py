# Load the configuration file
import sys
from lib.config_loader import load_config, get_config

if len(sys.argv) != 2:
    print("The configuration file is required in parameter.")
    print("For example, 'python3 %s config.json'" % sys.argv[0])
    sys.exit(2)
load_config(sys.argv[1])

from database.base import DB_URL
from database.connector import create_tables
import logging

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Create the tables from the URL '%s'" % DB_URL)
if create_tables():
    logging.info("Database initialization complete")
else:
    logging.error("Fail to initialize the database")
