
# Insert new-line-delimited, stringified JSON entries from file into mongodb collection

import json
import urllib2
from pymongo import MongoClient

client = MongoClient()
collection = client['data-helpers']['entries']

composite_file_url = 'https://storage.googleapis.com/jarvis123192/hn_compose'

entries = [entry for entry in urllib2.urlopen(composite_file_url).read().split("\n") if entry.strip() != '']

collection.insert(json.loads('[' + ','.join(entries) + ']'))
