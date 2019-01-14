import json
import sys
from time import localtime, strftime
from avro import schema, datafile, io
from GCPStorage import move_file_gcp
from GCPBigQ_IngestURI import load_bq_from_uri
from YelpAPI_Search import call_yelp_api_business_search
from GCPBigQ_ChicagoXREF import load_bq_chicago_xref
from GCPBigQ_IngestLocalFile import load_bq_from_local_file
from time import sleep

# Date string for output file
snapshot_date_local = localtime()
snapshot_date = strftime("%Y-%m-%d", snapshot_date_local)
snapshotDateStringFile = strftime("%Y%m%d_%H%M%S", snapshot_date_local)

# Read Config File and set global variables
config_file = sys.argv[2]
#config_file = r"C:\Users\jzamacona\PycharmProjects\SocialMediaAnalytics\config.json"
with open(config_file, "r+") as fo:
    read_config_file = fo.read()
    parsed_json = json.loads(read_config_file)
    avro_schema_file = parsed_json["avro_schema"]
    avro_output = parsed_json["avro_output"]
    yelp_api_key = parsed_json["api_key_yelp"]
    gcp_auth_method = parsed_json["gcp_auth_method"]
    gcp_key = parsed_json["gcp_key"]
    gcp_bucket_name = parsed_json["gcp_bucket_name"]
    gcp_output = parsed_json["gcp_output"]
    gcp_dataset = parsed_json["gcp_bq_dataset"]
    gcp_table = parsed_json["gcp_bq_table"]
    gcp_uri_source = parsed_json["gcp_bq_uri_source"]
    chicago_xref_file = parsed_json["chicago_xref_local_file"]
    gcp_table_chicago_xref = parsed_json["gcp_chicago_xref_table"]
    gcp_dataset_chicago_xref = parsed_json["gcp_chicago_xref_dataset"]

avro_outfile_name = avro_output + snapshotDateStringFile + '.avro'
gcp_outfile_name = gcp_output + snapshotDateStringFile + '.avro'
gcp_uri_full_name = gcp_uri_source+gcp_outfile_name

# AVRO PROCESSING
fa = open(avro_schema_file, "r+")
schema_str = fa.read()
fo.close()
fa.close()
schema_parse = schema.Parse(schema_str)

# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(schema_parse)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
    open(avro_outfile_name, 'wb'),
    rec_writer,
    writer_schema=schema_parse
)

# Read Yelp API Parameters JSON file
params_file = sys.argv[1]
run_params_file = open(params_file)
#run_params_file = open(r"C:\Users\jzamacona\PycharmProjects\SocialMediaAnalytics\run_params.json")
run_params_str = run_params_file.read()
run_params_data = json.loads(run_params_str)

# Set Yelp API Parameters Variables
location_list = run_params_data.get('location')
sort_list = run_params_data.get('sort_by')
categories_list = run_params_data.get('categories')
term_param = run_params_data.get('term')
price_param = run_params_data.get('price')
attributes_param = run_params_data.get('attributes')
radius_param = run_params_data.get('radius')
records_param = run_params_data.get('records')

# Initialize loop conditions
offset_count = int(records_param/50)
if offset_count == 0:
    offset_count = 1
    limit = records_param
    offset = 0
else:
    offset = 0
    limit = 50

# Loop through the data
for i in range(offset_count):
    for location, category, sort in [(location, category, sort)
                                     for location in location_list
                                     for category in categories_list
                                     for sort in sort_list]:
        # Build parameters string and call YELP API Function
        passed_params = {'location': location,
                         'categories': category,
                         'sort_by': sort,
                         'offset': offset,
                         'limit': limit,
                         'price': price_param,
                         'attributes': attributes_param,
                         'radius': radius_param,
                         'term': term_param}
        yelp_api_raw_output = call_yelp_api_business_search(yelp_api_key, passed_params)
        yelp_api_json_output = yelp_api_raw_output.text
        yelp_api_parsed_output = json.loads(yelp_api_json_output)
        yelp_api_output = yelp_api_parsed_output["businesses"]

        # Parse through the JSON output from the API call above
        for result in yelp_api_output:
            location_value = location
            category_value = category
            sort_value = sort
            raw_url = result["url"]
            raw_id = result["id"]
            raw_display_phone = result['display_phone']
            raw_review_count = result["review_count"]
            raw_rating = result["rating"]
            raw_categories = result["categories"]
            raw_coordinates = result['coordinates']
            raw_alias = result["alias"]
            raw_transactions = result["transactions"]
            raw_name = result["name"]
            raw_is_closed = result["is_closed"]
            raw_location = result["location"]
            raw_image_url = result['image_url']
            try:
                raw_price = result["price"]
            except:
                raw_price = 'u'
            raw_distance = result["distance"]
            raw_phone = result["phone"]
            if raw_categories:
                raw_business_categories_title = ''
                raw_business_categories_alias = ''
                for business_category in raw_categories:
                    raw_business_categories_title = raw_business_categories_title + (
                        str(business_category['title'])) + ','
                    raw_business_categories_alias = raw_business_categories_alias + (
                        str(business_category['alias'])) + ','
                raw_business_categories_title = raw_business_categories_title[:-1]
                raw_business_categories_alias = raw_business_categories_alias[:-1]
            else:
                raw_business_categories_title = 'UNKNOWN'
                raw_business_categories_alias = 'UNKNOWN'

            # Store Output: AVRO
            df_writer.append({"snapshot_date": snapshot_date,
                              "location_value": location_value,
                              "category_value": category_value,
                              "sort_value": sort_value,
                              "records_value": records_param,
                              "name": raw_name,
                              "url": raw_url,
                              "id": raw_id,
                              "display_phone": raw_display_phone,
                              "review_count": raw_review_count,
                              "rating": raw_rating,
                              "coordinates": raw_coordinates,
                              "coordinates.latitude": raw_coordinates["latitude"],
                              "coordinates.longitude": raw_coordinates["longitude"],
                              "alias": raw_alias,
                              "transactions": raw_transactions,
                              "categories": raw_categories,
                              "categories.business_title_list": raw_business_categories_title,
                              "categories.business_alias_list": raw_business_categories_alias,
                              "location.address1": raw_location["address1"],
                              "location.address2": raw_location["address2"],
                              "location.address3": raw_location["address3"],
                              "location.country": raw_location["country"],
                              "location.city": raw_location["city"],
                              "location.state": raw_location["state"],
                              "location.zip_code": raw_location["zip_code"],
                              "location.display_address": raw_location["display_address"],
                              "image_url": raw_image_url,
                              "price": raw_price,
                              "distance": raw_distance,
                              "phone": raw_phone
                              })
    offset = offset + 50
    sleep(10)
df_writer.close()

# Move file to GCP Storage
move_file_gcp(gcp_bucket_name, gcp_outfile_name, avro_outfile_name, gcp_key, gcp_auth_method)

# Load file to GCP BigQuery
load_bq_from_uri(gcp_uri_full_name, "WRITE_TRUNCATE", gcp_dataset, gcp_table, gcp_key, gcp_auth_method)


# CHICAGO COMMUNITY XREF
sql_xref = """
SELECT distinct R.ID, R.coordinates_latitude, R.coordinates_longitude, XREF.COMMUNITY, r.review_count 
FROM `firstproject-218715.QA_Env.RAW_YELP_SEARCH` R
LEFT JOIN `firstproject-218715.QA_Env.XREF_CHICAGO_RSTR_NEIGHBORHOOD` XREF
ON R.ID=XREF.RESTAURANT_YELP_ID
WHERE XREF.COMMUNITY is null
and R. location_value=R. location_zip_code
ORDER by r.review_count DESC
"""

load_bq_chicago_xref(sql_xref, chicago_xref_file, gcp_key, gcp_auth_method)
load_bq_from_local_file(chicago_xref_file, "WRITE_APPEND", gcp_dataset_chicago_xref, gcp_table_chicago_xref, gcp_key, gcp_auth_method)
