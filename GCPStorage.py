from google.cloud import storage


def move_file_gcp(target_bucket, target_path, source_file, gcp_auth_key=None, authentication='gcp_account'):
    if authentication == 'local':
        storage_client = storage.Client.from_service_account_json(gcp_auth_key)
    else:
        storage_client = storage.Client()
    bucket = storage_client.get_bucket(target_bucket)
    blob = bucket.blob(target_path)  # Name of file when it lands on GPC Bucket
    blob.upload_from_filename(filename=source_file)  # Local File Path and Name

