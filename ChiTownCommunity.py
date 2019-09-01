import requests
import json

def get_chi_community(lon, lat):
    url_dyn = 'https://data.cityofchicago.org/resource/igwz-8jzy.json?$where=intersects(the_geom,%20%27POINT%20({}%20{})%27)'.format(lon, lat)
    response = requests.request('GET', url_dyn)
    json_string = response.text
    parsed_json = json.loads(json_string)
    try:
        community = parsed_json[0]['community']
    except:
        community = 'UNKNOWN'
    return community

print(get_chi_community(-87.704650303852,41.923137314789))

#[-87.718238078095,41.953877541087]
#[-87.716443878231,41.787057792673]
#[-87.662784060841,41.731235342311]
#[-87.714772641696,41.794855090865]