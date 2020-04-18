import requests
import json
import logging
import boto3
from botocore.exceptions import ClientError


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket_exists = s3_resource.Bucket('open-library') in s3_resource.buckets.all()
bucket_name = "open-library"
file_name = "books-by-subject/python_book.json"

def create_bucket(bucket_name, region=None):
    try:
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_python_books():
    base_url = 'http://openlibrary.org/search.json?subject=python'
    try:
        r = requests.get(base_url)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    return r.json()


def store_file(file_content, bucket, key):
    try:
        s3_client.put_object(Body=file_content, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.exception('Failed to store the file') 
        raise


if bucket_exists is False:
    create_bucket('open-library', 'sa-east-1')
store_file(json.dumps(get_python_books()), 'bucket', file_name)
