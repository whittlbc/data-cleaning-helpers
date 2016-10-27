from os import listdir
from os.path import isfile, join
import json
import sys

if len(sys.argv) < 2:
	raise "No data directory specified\nUsage: $ python combine_nld_json my_data_dir"

data_path = sys.argv[1]
files = [f for f in listdir(data_path) if isfile(join(data_path, f))]

entries_map = {}

for file in files:

	with open('{}/{}'.format(data_path, file), 'r') as f:

		for str_entry in f.read().split("\n"):

			if str_entry.strip():
				json_entry = json.loads(str_entry)
				entries_map[json_entry['id']] = json_entry
				
		f.close()


with open('data.json', 'w+') as f:
	json.dump(entries_map, f)
