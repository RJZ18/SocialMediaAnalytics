from google.cloud import bigquery


def load_bq_from_uri(file_uri, write_disposition, dataset_id, table_id, gcp_auth_key=None, authentication='gcp_account'):
    if authentication == 'local':
        bigquery_client = bigquery.Client.from_service_account_json(gcp_auth_key)
    else:
        bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO
    job_config.write_disposition = write_disposition
    bq_job = bigquery_client.load_table_from_uri(
            file_uri,
            table_ref,
            job_config=job_config
             )
    return bq_job.result()


