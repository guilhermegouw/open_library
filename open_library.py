import requests
import json

base_url = 'http://openlibrary.org/'
subject_python = 'search.json?subject=python'

r = requests.get(base_url + subject_python)

dict_result = r.json()
json_result = json.dumps(dict_result)

with open('json_result', 'w') as json_file:
    json.dump(json_result, json_file)
