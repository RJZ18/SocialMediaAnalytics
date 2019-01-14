from google.cloud import bigquery
import csv
import requests
import json
from time import sleep

def load_bq_chicago_xref(sql, local_file, gcp_auth_key=None, authentication='gcp_account'):
    if authentication == 'local':
        bigquery_client = bigquery.Client.from_service_account_json(gcp_auth_key)
    else:
        bigquery_client = bigquery.Client()

    query_job = bigquery_client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query.
        location='US')  # API request - starts the query
    counter = 0

    with open(local_file, mode='w', newline='\n') as community_file:
        community_writer = csv.writer(community_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        community_writer.writerow(["RESTAURANT_YELP_ID", "COMMUNITY"])
        for row in query_job:  # API request - fetches results
            restaurant_id = str(row[0])
            lat = row[1]
            lon = row[2]
            url_dyn = 'https://data.cityofchicago.org/resource/igwz-8jzy.json?$where=intersects(the_geom,%20%27POINT%20({}%20{})%27)'.format(
                lon, lat)
            response = requests.request('GET', url_dyn)
            json_string = response.text
            parsed_json = json.loads(json_string)
            try:
                community = parsed_json[0]['community']
            except:
                community = 'UNKNOWN'
            sleep(2)
            community_writer.writerow([restaurant_id, community])
    return 0
