from get_connection import get_connection

conn = get_connection()
cursor = conn.cursor()
with open("./setup_tables.sql") as f:
    cursor.execute(f.read())

with open("./setup_view.sql") as f:
    cursor.execute(f.read())

conn.commit()
conn.close()
