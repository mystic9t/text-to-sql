"""utils to help make generic connections for databases
"""

from os import getenv
from dotenv import load_dotenv

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
        "dbname": getenv("dbname"),
        "user": getenv("db_user"),
        "password": getenv("db_password"),
        "host": getenv("db_host"),
        "port": getenv("db_port"),
    }
    # start connection
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        print("Database connection established")
        return conn, cur
    except Exception as e:
        print(f"Connection Failed. {e}")
        return 0, 0


def connection_end(cur, conn):
    """_summary_

    Args:
        cur (_type_): _description_
        conn (_type_): _description_

    Returns:
        _type_: _description_
    """
    cur.close()
    conn.close()
    return print("Database connection closed")
