import requests
import json
import logging
import boto3
from botocore.exceptions import ClientError


def get_json_result():
    base_url = 'http://openlibrary.org/search.json?subject=python'
    try:
        r = requests.get(base_url)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    return r.json()


with open('json_result.json', 'w') as json_file:
    json.dump(get_json_result(), json_file)


# Create AWS S3 Bucket
def create_bucket(bucket_name, region=None):
    try:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


s3 = boto3.resource('s3')
bucket_exists = s3.Bucket('open-library') in s3.buckets.all()

if bucket_exists:
    try:
        s3.Object('open-library', 'json_result.json').load()
        print("File already exists inside this bucket.")
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3_client = boto3.client('s3')
            with open("json_result.json", "rb") as f:
                s3_client.upload_fileobj(f, "open-library", "json_result.json")
else:
    s3_client = boto3.client('s3')
    create_bucket('open-library', 'sa-east-1')
    with open("json_result.json", "rb") as f:
        s3_client.upload_fileobj(f, "open-library", "json_result.json")
        print("File successfully uploaded.")
