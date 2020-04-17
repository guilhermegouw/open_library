import json

import boto3
import psycopg2


# Download from S3
s3_client = boto3.client('s3')
s3_client.download_file('open-library', 'json_result.json', 'json_result.json')


# Creating table

conn = psycopg2.connect(user="postgres",
                    password="postgres",
                    host="localhost",
                    port="5433",
                    database="postgres",)

cursor = conn.cursor()
cursor.execute("select * from information_schema.tables where table_name=%s", ('books',))
table_exists = bool(cursor.rowcount)

if table_exists is False:
    try:
        create_table_books = '''CREATE TABLE books
        (KEY TEXT PRIMARY KEY,
        TITLE TEXT NOT NULL,
        SUBJECT TEXT [],
        FIRSTPUBLISH TEXT NOT NULL);'''

        cursor.execute(create_table_books)
        conn.commit()
        print("Table created successfully in PostgreSQL")
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL table", error)
else:
    print('Table already exists.')

# open the file
with open('json_result.json', 'r') as json_file:
    data = json.load(json_file)

book_data = data['docs']

# Insert data from file to table
try:
    insert_books = """INSERT INTO books (KEY, TITLE, SUBJECT, FIRSTPUBLISH) VALUES (%s, %s, %s, %s)"""
    for book in book_data:
        record_to_insert = (book['key'], book['title'], book['subject'], book['first_publish_year'])
        cursor.execute(insert_books, record_to_insert)
    conn.commit()
    print("Data successfully inserted into books table")
except (Exception, psycopg2.Error) as error:
    if conn:
        print("Failed to insert record into books table", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed.")
