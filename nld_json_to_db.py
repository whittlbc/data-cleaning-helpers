from os import listdir
from os.path import isfile, join
import json
import sys
from pymongo import MongoClient

client = MongoClient('mongodb://data-helpers:data-helpers@localhost:<port>/data-helpers')
collection = client['data-helpers']['entries']

if len(sys.argv) < 2:
	raise "No data directory specified\nUsage: $ python combine_nld_json my_data_dir"

data_path = sys.argv[1]
files = [f for f in listdir(data_path) if isfile(join(data_path, f))]

for file in files:
	with open('{}/{}'.format(data_path, file), 'r') as f:
		collection.insert(json.loads('[' + ','.join(filter(lambda x: x != '', f.read().split("\n"))) + ']'))
		f.close()
