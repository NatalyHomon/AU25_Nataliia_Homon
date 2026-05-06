import boto3
import pytest
import requests
from botocore import UNSIGNED
from botocore.config import Config
from google.cloud import storage
from botocore.exceptions import ClientError

@pytest.fixture(scope='function')
def provide_config():
    config = {'prefix': '2024/01/01/KTLX/', 'gcp_bucket_name': "gcp-public-data-nexrad-l2",
              'aws_bucket_name': 'unidata-nexrad-level2',
              's3_anon_client': boto3.client('s3', config=Config(signature_version=UNSIGNED)),
              'gcp_storage_anon_client': storage.Client.create_anonymous_client()}
    return config


@pytest.fixture(scope='function')
def list_gcs_blobs(provide_config):
    config = provide_config
    blobs = config['gcp_storage_anon_client'].list_blobs(config['gcp_bucket_name'], prefix=config['prefix'])
    objects = [blob.name for blob in blobs]
    return objects




@pytest.fixture(scope='function')
def list_aws_blobs(provide_config):
    config = provide_config

    try:
        response = config['s3_anon_client'].list_objects(
            Bucket=config['aws_bucket_name'],
            Prefix=config['prefix']
        )

        objects = [content['Key'] for content in response.get('Contents', [])]
        return objects

    except ClientError as e:
        print("AWS error:", e.response["Error"])
        return []


@pytest.fixture(scope='function')
def provide_posts_data():
    return requests.get(
        "https://jsonplaceholder.typicode.com/posts?userId=3")

