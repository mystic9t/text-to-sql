"""
contains SQL queries to be utilized
"""
CREATE_SCHEMA ="""
CREATE SCHEMA IF NOT EXISTS {}
"""

GET_TABLES = """
SELECT DISTINCT table_name
FROM information_schema.tables
WHERE table_schema = {}
"""

GET_COLUMNS = """
SELECT DISTINCT column_name, data_type
FROM FROM information_schema.columns
WHERE table_schema = {}
AND table_name = {}
"""

GET_CARDINALS = """
with cte as (
    SELECT count(DISTINCT {column_name}) AS Cardinality
    FROM {schema_name}.{table_name}
)
SELECT DISTINCT {column_name}
FROM {schema_name}.{table_name}
WHERE (SELECT Cardinality FROM cte) <= 20
"""