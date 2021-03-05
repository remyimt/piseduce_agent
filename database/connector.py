from database.base import Base, DB_URL, engine, SessionLocal
from sqlalchemy import inspect
# Import tables to load the table description
import database.tables, logging


def open_session():
    return SessionLocal()


def row2props(alchemyResults):
    props = {}
    for ar in alchemyResults:
        if "prop_name" in ar.__dict__:
            props[ar.prop_name] = ar.prop_value
        else:
            logging.warning("Tuple does not have 'prop_name' key (%s)" % ar)
    return props


def close_session(session):
    session.commit()
    session.close()


def create_tables():
    inspector = inspect(engine)
    tables = []
    if DB_URL.startswith("sqlite"):
        tables = inspector.get_table_names()
    else:
        db_name = DB_URL.split('/')[-1]
        for sch in inspector.get_schema_names():
            if sch == db_name:
                tables = inspector.get_table_names(schema=sch)
    if len(tables) == 0:
        logging.info("The database is empty. Create tables...")
        Base.metadata.create_all(engine)
        return True
    return False
