"""
contains utility functions realted to data loading and clean up
"""

import json
import os
import re
from typing import Dict

import numpy as np
import pandas as pd
from sqlalchemy import Column, DateTime, Float, Integer, String, Table, inspect
from sqlalchemy.sql import text

from constants import sql_query
from utils import connection_utils

CONFIG_PATH = "config/config.json"


def load_csv_to_df(
    data_path: str = "data_tables",
) -> Dict[str, pd.DataFrame]:
    """
    Loads all CSV files from a specified folder and assigns each to a different DataFrame.

    Parameters:
    data_path (str): The path to the folder containing the CSV files.
    """
    # List all files in the directory
    files = os.listdir(data_path)
    # Load each CSV file and assign to a DataFrame
    dataframes = {}
    for file in files:
        if file.endswith(".csv"):
            df_name = os.path.splitext(file)[0]
            dataframes[df_name] = pd.read_csv(os.path.join(data_path, file))
    return dataframes


def sanitize_column_name(col_name: str) -> str:
    """
    Sanitizes a DataFrame column name by removing special characters
    and replacing spaces with underscores.
    """
    # Remove special characters and replace spaces with underscores
    return re.sub(r"\W+", "_", col_name)


def convert_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the datatype of DataFrame columns based on configurations
    loaded from config.json. Converts columns to datetime if their names
    contain any of the words provided in date_time_list. Converts columns
    to object (string) if their names contain any of the words provided
    in key_list.
    """
    # Load configurations from config.json
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)
        dtype_list = config.get("dtype_list", {})
        date_time_list = dtype_list.get("date_time_list", [])
        key_list = dtype_list.get("key_list", [])

    # Iterate through each column in the DataFrame
    for column in df.columns:
        # Check if any of the words in date_time_list are in the column name
        if any(word in column for word in date_time_list):
            df[column] = pd.to_datetime(df[column])

        # Check if any of the words in key_list are in the column name
        if any(word in column for word in key_list):
            df[column] = df[column].astype(str)
    # Replace NaN and NaT with None
    df = df.replace({np.NaN: None})

    return df


def create_table_from_dataframe(
    df: pd.DataFrame, table_name: str, engine, metadata_obj
) -> None:
    """creates a Table from df using SQLAlchemy and basic cursor connection"""
    # Sanitize column names
    sanitized_columns = {col: sanitize_column_name(col) for col in df.columns}
    df = df.rename(columns=sanitized_columns)

    # Check for date/time and ID/keys columns
    df = convert_columns(df)

    # Dynamically create columns based on DataFrame columns and data types
    columns = [
        Column(
            col,
            (
                String
                if dtype == "object"
                else (
                    Integer
                    if dtype == "int64"
                    else (
                        Float
                        if dtype == "float64"
                        else DateTime if dtype == "datetime64[ns]" else String
                    )
                )
            ),
        )  # Default to String for unrecognized types
        for col, dtype in zip(df.columns, df.dtypes)
    ]

    # Create a table with the defined columns
    table = Table(table_name, metadata_obj, *columns)

    # Create the table in the database
    metadata_obj.create_all(engine)

    # Insert data from DataFrame into the table
    with engine.connect() as conn:
        for _, row in df.iterrows():
            insert_stmt = table.insert().values(**row.to_dict())
            conn.execute(insert_stmt)
        conn.commit()


def extract_cardinals(data_extract_loc: str) -> None:
    """Extract distinct values of string columns from tables
    in the schema given in config
    """
    # Getting keywords for IDs/Keys
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)
        dtype_list = config.get("dtype_list", {})
        skip_list = dtype_list.get("skip_list", [])
        driver_name = config.get("driver_name", "")
        schema_name = config.get("schema_name", "")

    ## Creating SQLAlchemy engine to load tables in Database
    engine, metadata_obj = connection_utils.sql_engine(driver_name, schema_name)
    # Get Metadata
    metadata_obj.reflect(bind=engine)
    # Initialize an inspector
    inspector = inspect(engine)

    # Get the names of all tables in the schema
    table_names = inspector.get_table_names(schema=schema_name)

    # Create a dictionary with table names as keys and list of columns as values
    tables_columns_dict = {}
    for table_name in table_names:
        columns = inspector.get_columns(table_name, schema=schema_name)
        column_info = {}
        for column in columns:
            column_name = column["name"]
            column_type = column["type"]
            # Skip column if it contains any of the skip_column_keywords
            if any(keyword in column_name for keyword in skip_list):
                column_info[column_name] = [str(column["type"])]
            # Check if the column is a string type
            elif (
                "CHAR" in str(column_type).upper() or "TEXT" in str(column_type).upper()
            ):
                # Query to get distinct values
                with engine.connect() as connection:
                    result = connection.execute(
                        text(
                            sql_query.GET_CARDINALS.format(
                                column_name=column_name,
                                schema_name=schema_name,
                                table_name=table_name,
                            )
                        )
                    ).mappings()  # Fetch results as a list of dictionaries
                    distinct_values = [row[column_name] for row in result]
                column_info[column_name] = [str(column["type"])] + distinct_values
            else:
                column_info[column_name] = [str(column["type"])]
        tables_columns_dict[table_name] = column_info

    # Write the result to a JSON file
    output_file = data_extract_loc
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tables_columns_dict, f, indent=4)

    return print(f"Data extracted into {output_file}")
