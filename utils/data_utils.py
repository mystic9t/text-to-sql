"""
contains utility functions realted to data loading and clean up
"""

import json
import re
from sqlalchemy import Column, String, Integer, DateTime, Float
import pandas as pd

CONFIG_PATH = "config/config.json"


def sanitize_column_name(col_name):
    """
    Sanitizes a DataFrame column name by removing special characters
    and replacing spaces with underscores.

    Parameters:
    col_name (str): The column name to be sanitized.

    Returns:
    str: The sanitized column name.

    Example:
    >>> sanitize_column_name('Column Name 123!')
    'Column_Name_123_'
    """
    # Remove special characters and replace spaces with underscores
    return re.sub(r"\W+", "_", col_name)


def convert_columns_to_datetime(df):
    """
    Converts the datatype of DataFrame columns to datetime if their names
    contain any of the words provided in date_time_list.

    Parameters:
    df (pd.DataFrame): The DataFrame whose columns need to be converted.
    """
    # Load word_list from config.json
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)
        date_time_list = config.get("date_time_list", [])
    # Iterate through each column in the DataFrame
    for column in df.columns:
        # Check if any of the words in the word_list are in the column name
        if any(word in column for word in date_time_list):
            # Convert the column to datetime
            df[column] = pd.to_datetime(df[column])
            print(f"Converted column '{column}' to datetime.")

    return df


def create_table_from_dataframe(
    df: pd.DataFrame, table_name: str, engine, metadata_obj
):
    """creates a Table from df using SQLAlchemy and basic cursor connection

    Args:
        df (pd.DataFrame): _description_
        table_name (str): _description_
        engine (_type_): _description_
        metadata_obj (_type_): _description_
    """
    # Sanitize column names
    sanitized_columns = {col: sanitize_column_name(col) for col in df.columns}
    df = df.rename(columns=sanitized_columns)

    # Check for date time columns
    df = convert_columns_to_datetime(df)
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
