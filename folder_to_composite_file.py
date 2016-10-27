import os

os.system('gsutil ls gs://jarvis123192/hn >> files.txt')

with open('files.txt') as f:
	files = ' '.join(f.read().split("\n")[1:])
	f.close()
	
os.system('gsutil compose {} gs://jarvis123192/hn_compose'.format(files))

print 'Created composite file at gs://jarvis123192/hn_compose.'