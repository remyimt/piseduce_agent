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
