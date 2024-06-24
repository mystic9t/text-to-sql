"""
utils to help make generic connections for databases
"""

from os import getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import URL

import psycopg2

ENV_PATH = ".env"


def connection_start():
    """_summary_

    Returns:
        _type_: _description_
    """
    load_dotenv(ENV_PATH)
    # connection parameters
    db_params = {
        "dbname": getenv("db_name"),
        "user": getenv("db_user"),
        "password": getenv("db_password"),
        "host": getenv("db_host"),
        "port": getenv("db_port"),
    }
    # start connection
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Connection Failed. {e}")
        return 0, 0


def connection_end(cur, conn):
    """_summary_

    Args:
        cur (_type_): _description_
        conn (_type_): _description_
    """
    cur.close()
    conn.close()


def sql_engine(drivername, schema_name):
    """_summary_

    Args:
        drivername (_type_): _description_
        schema_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    # connection URL
    db_url = URL.create(
        drivername=drivername,
        username=getenv("db_user"),
        password=getenv("db_password"),
        host=getenv("db_host"),
        port=getenv("db_port"),
        database=getenv("db_name"),
    )
    # Creating engine and MetaData with schema
    engine = create_engine(db_url)
    metadata_obj = MetaData(schema=schema_name)
    return engine, metadata_obj
