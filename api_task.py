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


from botocore.exceptions import ClientError

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
    response = requests.get("https://jsonplaceholder.typicode.com/posts?userId=3")
    assert response.status_code == 200
    data = response.json()

    return data


def test_user_with_posts(provide_posts_data):

    assert len(provide_posts_data) == 10



def test_data_is_presented_between_staging_raw(list_gcs_blobs, list_aws_blobs):
    assert list_gcs_blobs, "GCP bucket is empty"
    assert list_aws_blobs, "AWS bucket is empty"