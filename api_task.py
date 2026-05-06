
def test_user_with_posts(provide_posts_data):
    response = provide_posts_data

    assert response.status_code == 200, (
        f"Expected status code 200, but got {response.status_code}"
    )

    data = response.json()

    assert len(data) == 10, (
        f"Expected 10 posts, but got {len(data)}"
    )



def test_data_is_presented_between_staging_raw(list_gcs_blobs, list_aws_blobs):
    assert list_gcs_blobs, "GCP bucket is empty"
    assert list_aws_blobs, "AWS bucket is empty"