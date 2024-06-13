from psycopg2 import sql, extras

def add_schema(conn, cur, schema):
    query = f""" 
    IF NOT EXISTS (
        SELECT 1
        FROM pg_namespace
        WHERE nspname = '{schema}'
    ) THEN
        EXECUTE 'CREATE SCHEMA {schema}';
    END IF;
    """
    cur.execute(query)
    conn.commit()

def add_table(conn, cur, schema, table, df):
    # dropping table if already exits
    drop_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    """
    cur.execute(drop_query)
    conn.commit()

    # mapping data types
    dtypes_mapping = {
        "int64": "FLOAT",
        "float64": "FLOAT",
        "object": "TEXT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "DATE"
    }
    # extracting column names along with data types
    columns = ",".join(
        [
            f"{col} {dtypes_mapping[str(dtype)]}"
            for col,dtype in zip(df.columns, df.dtypes)
        ]
    )

    # creating empty table
    create_query = f"""
    CREATE TABLE {schema}.{table} ({columns});
    """
    cur.execute(create_query)
    conn.commit()

    # inserting data in new table
    insert_query = f"""
    INSERT INTO {schema}.{table} ({",".join(df.columns)}) VALUES %s
    """
    args = [tuple(x) for x in df.to_numpy()]
    extras.execute_values(cur, insert_query, args)
    conn.commit()

    return f"Data for {schema}.{table} inserted into the database"
