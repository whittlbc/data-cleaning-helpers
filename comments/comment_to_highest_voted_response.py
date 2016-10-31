from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor
import sys
import code
import csv
import json


# Ensure required args passed in
if len(sys.argv) != 2:
	raise BaseException("Invalid args.\nUsage: $ python comment_to_highest_voted_response.py <dbname>")

dbname = sys.argv[1]
con = connect(dbname=dbname)
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor(cursor_factory=DictCursor)

col_names = ['id', 'by', 'author', 'time', 'time_ts', 'text', 'parent', 'deleted', 'dead', 'ranking']
num_cols = len(col_names)

cur.execute("""\
SELECT * FROM resources AS resource
INNER JOIN resources AS child
ON resource.id = child.parent
WHERE resource.text IS NOT NULL AND resource.dead IS NOT TRUE
AND child.text IS NOT NULL AND child.dead IS NOT TRUE
ORDER BY child.ranking DESC
""")

comment_map = {}
comment_response = []

for pairing in cur:
	comment = pairing[:num_cols]
	child = pairing[num_cols:]

	id_index = col_names.index('id')
	ranking_index = col_names.index('ranking')
	text_index = col_names.index('text')

	comment_id = str(comment[id_index])

	if not comment_map.get(comment_id):
		comment_map[comment_id] = child[ranking_index]

		comment_response.append({
			'comment': comment[text_index],
			'response': child[text_index]
		})
		
rankings = comment_map.values()
rankings.sort(reverse=True)
print rankings[:500]

# Write data to json file
with open('comment_response_raw.json', 'w+') as f:
	f.write(json.dumps(comment_response, sort_keys=True, indent=2))

# Write to csv as well
headers = comment_response[0].keys()

with open('comment_response_raw.csv', 'w+') as f:
	dict_writer = csv.DictWriter(f, headers)
	dict_writer.writeheader()
	dict_writer.writerows(comment_response)
