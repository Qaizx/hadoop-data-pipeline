from pyhive import hive

conn = hive.Connection(
    host="localhost",
    port=10000,
    username="root",
    database="finance"
)

cursor = conn.cursor()

cursor.execute("SHOW DATABASES")
print("Databases:", cursor.fetchall())

cursor.execute("SHOW TABLES")
print("Tables:", cursor.fetchall())

cursor.execute("SELECT COUNT(*) FROM expenses")
print(cursor.fetchall())


cursor.close()
conn.close()
