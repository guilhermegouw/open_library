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
create_table_subjects = '''CREATE TABLE subjects
        (ID SERIAL PRIMARY KEY,
        SUBJECT TEXT NOT NULL,
        BOOK_KEY TEXT NOT NULL,
        FOREIGN KEY (book_key) REFERENCES books(key) ON DELETE CASCADE);'''

cursor.execute(create_table_subjects)
conn.commit()
with open('json_result.json', 'r') as json_file:
    data = json.load(json_file)

book_data = data['docs']

# Insert data from file to table
try:
    insert_subjects = """INSERT INTO subjects (SUBJECT, BOOK_KEY) VALUES (%s, %s)"""
    for book in book_data:
        book_key = book['key']
        for subject in book['subject']:
            record_to_insert = (subject, book_key)
            cursor.execute(insert_subjects, record_to_insert)
    conn.commit()
    print("Data successfully inserted into books table")
except (Exception, psycopg2.Error) as error:
    if conn:
        print("Failed to insert record into subjects table", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed.")
