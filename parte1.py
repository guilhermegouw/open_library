import requests
import json


def get_json_result():
    base_url = 'http://openlibrary.org/search.json?subject=python'
    try:
        r = requests.get(base_url)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    return r.json()


with open('json_result.json', 'w') as json_file:
    json.dump(get_json_result(), json_file)
