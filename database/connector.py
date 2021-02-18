from database.base import Base, DB_URL, engine, SessionLocal
from sqlalchemy import inspect
import database.tables, logging


def open_session():
    return SessionLocal()


def row2dict(alchemyResult):
    return { c.key: getattr(alchemyResult, c.key) for c in inspect(alchemyResult).mapper.column_attrs }


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
