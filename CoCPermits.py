import requests
import json
import csv
from io import StringIO

local_file = r'C:\Users\rzamacona\Documents\Projects\CocAnalytics\DataSets\CocAnalyticsPermits.csv'
values = []
body = {'values': values}

def get_chi_permits():
    url='https://data.cityofchicago.org/resource/ydr8-5enu.csv?issue_date=2019-08-07T00:00:00.000'
    response = requests.request('GET', url)
    response_string = response.text
    parsed_csv = StringIO(response_string)
    reader = csv.reader(parsed_csv, delimiter=',')
    # json_string = response.text
    # parsed_json = json.loads(json_string)

    # return parsed_json
    return reader

for row in get_chi_permits():
    body['values'].append(row)
   # print('\t'.join(row))

print(body)

# with open(local_file, mode='a', newline='\n') as community_file:
#     community_writer = csv.writer(community_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#     for row in get_chi_permits():  # API request - fetches results
#         community_writer.writerow(row)
        #print(row)


#print(get_chi_permits())

# for row in get_chi_permits():
#     print('\t'.join(row))
