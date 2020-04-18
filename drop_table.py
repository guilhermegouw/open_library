import psycopg2


conn = psycopg2.connect(user="postgres",
                    password="postgres",
                    host="localhost",
                    port="5433",
                    database="postgres",)

cursor = conn.cursor()

tableName = 'books'

# dropTableStmt = "DROP TABLE books;"

dropTableStmt_2 = "DROP TABLE subjects;"
dropTableStmt = "DROP TABLE books;"


cursor.execute(dropTableStmt_2)

cursor.execute(dropTableStmt)

conn.commit()
cursor.close()
conn.close()
