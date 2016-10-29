from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import re


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
		
		for cell in row:
			if not data_types[col_index] and cell.strip():
				data_types[col_index] = determine_data_type(cell.strip())

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
	
	# Determine the primary key (or if you'll need to add one)
	primary_key = determine_primary_key(headers)
	
	sql_schema = []
	
	# If primary column needs to be added, do that.
	if primary_key in ['row', 'row_num']:
		headers = [primary_key] + headers
		data_types = ['int'] + data_types
		
	i = 0
	for col_name in headers:
		sql_col = '{} {}'.format(col_name, data_types[i])
		
		if col_name == primary_key:
			sql_col += ' PRIMARY KEY NOT NULL'
		
		sql_schema.append(sql_col)
		i += 1
		
	return ', '.join(sql_schema)
	

def create_table(db, csv):
	con = get_db_connection(db)
	cur = con.cursor()
	
	if table_exists(cur, 'resources'):
		error("Error: Table 'resources' already exists.")
	
	# Create a schema based on our csv headers and data
	# (Note: CSV must be comma-delimited for now)
	schema = create_schema(csv)
	
	# Create our 'resources' table
	cur.execute('CREATE TABLE resources( {} );'.format(schema))

	leave_db(cur, con)
	

def error(message):
	raise BaseException(message)


# Ensure required args passed in
if len(sys.argv) not in [2, 3, 4]:
	error("Invalid args.\nUsage: $ python setup_pg.py <dbname> <path_to_csv>")


db_name = sys.argv[1].strip()
csv_path = sys.argv[2].strip()

create_db(db_name)
create_table(db_name, csv_path)