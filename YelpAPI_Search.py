import requests
from urllib.parse import quote


def call_yelp_api_business_search(api_key, url_params):
    host = 'https://api.yelp.com'
    path = '/v3/businesses/search'
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response