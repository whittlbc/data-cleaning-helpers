from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import re
import os


def get_db_connection(db):
	con = connect(dbname=db)
	con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	return con


def leave_db(cur, con):
	cur.close()
	con.close()


def database_exists(cur, name):
	cur.execute("SELECT EXISTS( SELECT * FROM pg_database WHERE datname = '{}' LIMIT 1 );".format(name))
	return cur.fetchone()[0]


def table_exists(cur, name):
	cur.execute("SELECT EXISTS( SELECT * FROM information_schema.tables WHERE table_name = '{}' );".format(name))
	return cur.fetchone()[0]


def create_db(db):
	con = get_db_connection('postgres')
	cur = con.cursor()
	
	# Raise an error if the DB already exists
	if database_exists(cur, db):
		error("Error: Database '{}' already exists.".format(db))
	
	# Create the new DB by name
	cur.execute('CREATE DATABASE {};'.format(db))

	# Close our connections
	leave_db(cur, con)
			

def get_data_types(data, num_cols):
	data_types = [None] * num_cols
	
	for row in data:
		col_index = 0
		
		for value in row:
			if not data_types[col_index] and value.strip():
				data_types[col_index] = determine_data_type(value.strip())

			col_index += 1
		
		if data_types.count(None) == 0:
			return data_types
		
	error('Error determining data types - One column never had any entries.')
			
		
def determine_data_type(data):
	# First check to see if data can be represented as an integer
	try:
		int(data)
		return 'int'
	except ValueError:
		# Not an integer...so either text or boolean
		if data in ['true', 'false']:
			return 'boolean'
		
		return 'text'
	

def determine_primary_key(headers):
	if 'id' in headers: return 'id'
	
	if 'row' in headers:
		return 'row_num'
	else:
		return 'row'


def create_schema(csv):
	# Download or open the csv
	if re.search('^(http|https|ftp)', csv):
		import urllib2
		file = urllib2.urlopen(csv)
	else:
		file = open(csv)
		
	# Turn string csv data into chart-style data
	data = map(lambda row: row.split(','), file.read().split("\n"))
	
	# Determine what your column names will be
	headers = map(lambda h: h.lower().strip(), data[0])
	data_rows = data[1:]
	
	# Determine data types for each column based on the data in the csv
	data_types = get_data_types(data_rows, len(headers))
	
	headers_with_type = []
	
	j = 0
	for header in headers:
		headers_with_type.append({
			'header': header,
			'type': data_types[j]
		})
		j += 1
	
	# Determine the primary key (or if you'll need to add one)
	primary_key = determine_primary_key(headers)
	
	sql_schema = []
	
	header_with_prim_key = headers
	
	# If primary column needs to be added, do that.
	if primary_key in ['row', 'row_num']:
		header_with_prim_key = [primary_key] + header_with_prim_key
		data_types = ['int'] + data_types
		
	i = 0
	for col_name in header_with_prim_key:
		sql_col = '{} {}'.format(col_name, data_types[i])
		
		if col_name == primary_key:
			sql_col += ' PRIMARY KEY NOT NULL'
		
		sql_schema.append(sql_col)
		i += 1
		
	schema = ', '.join(sql_schema)
	
	return schema, headers_with_type, data_rows
	

def create_table(db, csv):
	con = get_db_connection(db)
	cur = con.cursor()
	
	if table_exists(cur, 'resources'):
		error("Error: Table 'resources' already exists.")
	
	# Create a schema based on our csv headers and data
	# (Note: CSV must be comma-delimited for now)
	schema, headers_with_type, data_rows = create_schema(csv)
	
	# Create our 'resources' table
	cur.execute('CREATE TABLE resources( {} );'.format(schema))
	
	batch_size = 1000
	row_count = len(data_rows)
	
	if row_count < batch_size:
		insert_table_data(cur, headers_with_type, data_rows)
	else:
		batches = row_count / batch_size
		
		if row_count % batch_size:
			batches += 1
		
		for i in range(batches):
			range_start = batch_size * i
			range_end = batch_size * (i + 1)
			
			if range_end > row_count:
				batch = data_rows[range_start:]
			else:
				batch = data_rows[range_start:range_end]
		
			insert_table_data(cur, headers_with_type, batch)
			
			print "Inserted: {}%".format(((i + 1) / batches) * 100)

	leave_db(cur, con)


def insert_table_data(cur, headers, rows):
	data = []

	for row in rows:
		formatted_row = []
		
		i = 0
		for value in row:
			formatted_val = format_val(headers[i]['type'], value)
			formatted_row.append(formatted_val)
			i += 1
			
		data.append(tuple(formatted_row))
							
	header_names = ', '.join(map(lambda x: x['header'], headers))
	placeholders = ','.join(['%s'] * len(headers))
	
	values = ','.join(cur.mogrify('({})'.format(placeholders), r) for r in data)
	cur.execute('INSERT INTO resources ({}) VALUES {}'.format(header_names, values))


def format_val(type, val):
	# Handle what the 'null' value will be if value doesn't exist
	if not val:
		if type =='text': return ''
		return None
	
	# Value exists, so convert it to the correct type
	if type == 'int':
		val = int(val)
	elif type == 'boolean':
		val = bool(val)
	else:
		val = str(val)
		
	return val


def error(message):
	raise BaseException(message)


# Ensure required args passed in
if len(sys.argv) not in [2, 3, 4]:
	error("Invalid args.\nUsage: $ python csv_to_pg_dump.py <dbname> <path_to_csv>")

db_name = sys.argv[1].strip()
csv_path = sys.argv[2].strip()

create_db(db_name)
create_table(db_name, csv_path)
os.system('pg_dump {} | gzip > pgdump.gz'.format(db_name))
