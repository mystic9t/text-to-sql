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
SELECT DISTINCT {}
FROM {}.{}
ORDER BY 1
LIMIT 20
"""