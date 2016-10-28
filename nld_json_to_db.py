
# Insert new-line-delimited, stringified JSON entries from file into mongodb collection

import json
import urllib2
from pymongo import MongoClient

client = MongoClient()
collection = client['dataset-db']['resources']

file_url = 'https://storage.googleapis.com/jarvis123192/hn/hn0000000000'

suffix = [
	'02',
	'03',
	'04',
	'05',
	'06',
	'07',
	'08',
	'09',
	'10',
	'11',
	'12',
	'13',
	'14',
	'15',
	'16',
	'17',
	'18',
	'19',
	'20',
	'21',
	'22',
	'23',
	'24',
	'25',
	'26',
	'27',
	'28',
	'29',
	'30',
	'31'
]

for num in suffix:
	url = file_url + num
	entries = [entry for entry in urllib2.urlopen(url).read().split("\n") if entry.strip() != '']
	collection.insert(json.loads('[' + ','.join(entries) + ']'))
	entries = None