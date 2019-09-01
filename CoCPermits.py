import requests
import json

def get_chi_permits():
    url='https://data.cityofchicago.org/resource/ydr8-5enu.csv'
    response = requests.request('GET', url)
    csv_string = response.text
    #json_string = response.text
    #parsed_json = json.loads(json_string)

    #return parsed_json
    return csv_string


print(get_chi_permits())