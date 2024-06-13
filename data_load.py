from utils.connection_utils import connection_start, connection_end


q = '''
CREATE SCHEMA motor_store
'''
conn, cur = connection_start()
print(cur.execute(q))
conn.commit()

connection_end(cur, conn)
