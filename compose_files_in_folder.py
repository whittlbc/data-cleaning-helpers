
# Script used to compose all files in a gs folder into one file.
# Usage: $ python compose_files_in_folder my_bucket/my_folder my_bucket/my_composite_file

import os
import sys

if len(sys.argv) == 3:
	folder_path = sys.argv[1].strip()
	compose_file_path = sys.argv[2].strip()
	tmp_file_path = 'tmp/files.txt'
	
	if os.path.isfile(tmp_file_path):
		os.remove(tmp_file_path)
	
	os.system('gsutil ls gs://{} >> {}'.format(folder_path, tmp_file_path))
	
	with open(tmp_file_path) as f:
		files = ' '.join(f.read().split("\n")[1:])
	
	os.system('gsutil compose {} gs://{}'.format(files, compose_file_path))
