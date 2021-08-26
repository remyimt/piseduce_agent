from database.base import Base, DB_URL, engine, SessionLocal
from sqlalchemy import inspect
# Import tables to load the table description
import database.tables, logging


def open_session():
    return SessionLocal()


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
