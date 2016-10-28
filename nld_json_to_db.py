
# Insert new-line-delimited, stringified JSON entries from file into mongodb collection

import json
import urllib2
from pymongo import MongoClient

client = MongoClient()
collection = client['dataset-db']['resources']

file_url = 'https://storage.googleapis.com/jarvis123192/hn/hn000000000001'

entries = [entry for entry in urllib2.urlopen(file_url).read().split("\n") if entry.strip() != '']

collection.insert(json.loads('[' + ','.join(entries) + ']'))