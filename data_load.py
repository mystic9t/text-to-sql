"""
loads data from csv to SQL database and extracts table and column information into a json
"""

import json

from constants import sql_query
from utils import connection_utils, data_utils

CONFIG_PATH = "config/config.json"
LOAD_TABLES = False
EXTRACT_DATA = True
DATA_EXTRACT = "data_tables/data_extract.json"
SILO_DATA = "data_tables/silo"


def main() -> None:
    """
    main function
    """
    # Load configurations from config.json
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)
        driver_name = config.get("driver_name", "")
        schema_name = config.get("schema_name", "")

    if LOAD_TABLES:
        # Loading data from csv
        print("Loading Data ino DataFrames...\n")
        dataframes = data_utils.load_csv_to_df()
        # Loading Data to SQL Database
        ## Creating New Schema(if missing)
        print("Creating New Schema (if missing)")
        conn, cur = connection_utils.connection_start()
        foramtted_query = sql_query.CREATE_SCHEMA.format(schema_name)
        cur.execute(foramtted_query)
        conn.commit()
        connection_utils.connection_end(cur, conn)

        ## Creating SQLAlchemy engine to load tables in Database
        engine, metadata_obj = connection_utils.sql_engine(driver_name, schema_name)
        for name, df in dataframes.items():
            try:
                data_utils.create_table_from_dataframe(df, name, engine, metadata_obj)
                print(f"{name} loaded in Database")
            except Exception as e:  # pylint: disable=W0718
                print(f"unable to load {name}, error: {e}\n Skipping...")
    if EXTRACT_DATA:
        # Extracting Data from SQL Database
        data_utils.extract_cardinals(DATA_EXTRACT)


if __name__ == "__main__":
    main()
