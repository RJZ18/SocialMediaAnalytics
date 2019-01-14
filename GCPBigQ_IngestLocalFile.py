from google.cloud import bigquery


def load_bq_from_local_file(local_file, write_disposition, dataset_id, table_id, gcp_auth_key=None, authentication='gcp_account'):
    if authentication == 'local':
        bigquery_client = bigquery.Client.from_service_account_json(gcp_auth_key)
    else:
        bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    bigquery.LoadJobConfig().schema
    job_config_csv = bigquery.LoadJobConfig()
    job_config_csv.source_format = bigquery.SourceFormat.CSV
    job_config_csv.write_disposition = write_disposition
    job_config_csv.skip_leading_rows = 1
    job_config_csv.autodetect = True
    job_config_csv.schema = [
                                bigquery.SchemaField('RESTAURANT_YELP_ID', 'STRING'),
                                bigquery.SchemaField('COMMUNITY', 'STRING')
                            ]

    with open(local_file, 'rb') as source_file:
        job_csv = bigquery_client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config_csv
        )

    job_csv.result()

