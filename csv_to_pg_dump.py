from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import pandas
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
	
	print 'Creating database, {}...'.format(db)
	
	# Create the new DB by name
	cur.execute('CREATE DATABASE {};'.format(db))

	# Close our connections
	leave_db(cur, con)
			

def get_data_types(df, headers):
	data_types = []
	
	for header in headers:
		col_data = filter(lambda x: x != '', list(df.loc[:, header]))
		
		if not col_data:
			error('Column "{}" is completely empty.'.format(header))
		
		col_type = None
		
		for val in col_data:
			if col_type != 'text':
				try:
					int(val)
					col_type = 'int'
				except ValueError:
					if val.strip().lower() in ['true', 'false']:
						col_type = 'boolean'
					else:
						col_type = 'text'
	
		data_types.append(col_type)
	
	return data_types


def determine_data_type(data):
	try:
		int(data)
		dt = 'int'
	except ValueError:
		# Not an integer...so either text or boolean
		if data.lower() in ['true', 'false']:
			dt = 'boolean'
		else:
			dt = 'text'
	
	return dt


def read_csv(csv):
	data = pandas.read_csv(csv, sep=',', header=0, encoding='utf-8', dtype=str)
	data.fillna('', inplace=True)
	return data


def create_schema(csv):
	print 'Reading csv...'
	df = read_csv(csv)
	data = df.values
	headers = df.columns.values
	
	print 'Inferring datatypes...'
	
	data_types = get_data_types(df, headers)
	
	headers_with_type = []
	sql_schema = []

	i = 0
	for header in headers:
		dt = data_types[i]
		
		headers_with_type.append({
			'header': header,
			'type': dt
		})
		
		sql_schema.append('{} {}'.format(header, dt))
		i += 1
		
	schema = ', '.join(sql_schema)
	
	return schema, headers_with_type, data
	

def create_and_populate_table(db, csv):
	con = get_db_connection(db)
	cur = con.cursor()
	
	if table_exists(cur, 'resources'):
		error("Error: Table 'resources' already exists.")
				
	# Create a schema based on our csv headers and data
	# (Note: CSV must be comma-delimited for now)
	schema, headers_with_type, data = create_schema(csv)
	
	print 'Creating resources table...'.format(db)
	
	# Create our 'resources' table
	cur.execute('CREATE TABLE resources( {} );'.format(schema))
	
	batch_size = 5000
	row_count = len(data)
	
	print 'Populating resources table...'.format(db)
		
	if row_count < batch_size:
		insert_table_data(cur, headers_with_type, data)
	else:
		batches = row_count / batch_size
		
		if row_count % batch_size:
			batches += 1
		
		for i in xrange(batches):
			range_start = batch_size * i
			range_end = batch_size * (i + 1)
			
			if range_end > row_count:
				batch = data[range_start:]
			else:
				batch = data[range_start:range_end]
		
			insert_table_data(cur, headers_with_type, batch)
			
			print "Inserted batch {} of {}.".format((i + 1), batches)

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
	# if no value, return the empty type you want
	if not val:
		if type == 'text':
			return ''
		else:
			return None
	
	if type == 'int':
		val = int(val)
	elif type == 'boolean':
		val = bool(val)
	else:
		val = val.encode('utf-8')
	
	return val


def error(message):
	raise BaseException(message)


# Ensure required args passed in
if len(sys.argv) != 3:
	error("Invalid args.\nUsage: $ python csv_to_pg_dump.py <dbname> <path_to_csv>")

db_name = sys.argv[1].strip()
csv_path = sys.argv[2].strip()

create_db(db_name)
create_and_populate_table(db_name, csv_path)

print 'Dumping database...'

os.system('pg_dump {} | gzip > pgdump.gz'.format(db_name))

print 'Created pgdump.gz from {}.'.format(csv_path)