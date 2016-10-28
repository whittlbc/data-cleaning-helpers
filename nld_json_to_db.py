import os
import json
import urllib2
import sys
from pymongo import MongoClient


if len(sys.argv) == 2:
	client = MongoClient()
	collection = client['dataset-db']['resources']
	
	tmp_file = 'files.txt'
	
	os.system('gsutil ls gs://jarvis123192/{} >> {}'.format(sys.argv[1], tmp_file))
	
	with open(tmp_file) as f:
		urls = f.read().split("\n")[1:]
		f.close()
		
	os.remove(tmp_file)
		
	for url in urls:
		collection.insert(json.loads('[' + ','.join([e for e in urllib2.urlopen(url).read().split("\n") if e.strip() != '']) + ']'))