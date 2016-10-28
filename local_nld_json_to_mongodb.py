import json
from pymongo import MongoClient

client = MongoClient()
collection = client['dataset-db']['resources']

with open('hn_compose') as f:
	collection.insert(json.loads('[' + ','.join([e for e in f.read().split("\n") if e.strip() != '']) + ']'))
	f.close()
