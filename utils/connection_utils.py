"""
utils to help make generic connections for databases
"""

import os
from typing import Tuple

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import URL, Engine

ENV_PATH = ".env"


def connection_start():
    """Creating a basic psycopg2 connection"""
    load_dotenv(ENV_PATH)
    # connection parameters
    db_params = {
        "dbname": os.getenv("db_name"),
        "user": os.getenv("db_user"),
        "password": os.getenv("db_password"),
        "host": os.getenv("db_host"),
        "port": os.getenv("db_port"),
    }
    # start connection
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        return conn, cur
    except Exception as e:  # pylint: disable=W0718
        print(f"Connection Failed. {e}")
        return 0, 0


def connection_end(cur, conn) -> None:
    """ends curcor connection"""
    cur.close()
    conn.close()


def sql_engine(drivername: str, schema_name: str) -> Tuple[Engine, MetaData]:
    """Creating new SQLAlchemy Engine"""
    # connection URL
    db_url = URL.create(
        drivername=drivername,
        username=os.getenv("db_user"),
        password=os.getenv("db_password"),
        host=os.getenv("db_host"),
        port=os.getenv("db_port"),
        database=os.getenv("db_name"),
    )
    # Creating engine and MetaData with schema
    engine = create_engine(db_url)
    metadata_obj = MetaData(schema=schema_name)
    return engine, metadata_obj
