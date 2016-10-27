
# Insert new-line-delimited, stringified JSON entries from file into mongodb collection

import json
from pymongo import MongoClient

client = MongoClient()
collection = client['data-helpers']['entries']

with open('hn_compose') as f:
	entries = [entry for entry in f.read().split("\n") if entry.strip() != '']
	collection.insert(json.loads('[' + ','.join(entries) + ']'))
	f.close()
