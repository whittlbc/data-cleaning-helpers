
# Script used to clean a seq2seq dataset with the following processes:

# (1) Converts HTML entities and character references to ordinary characters
# (2) Strip out HTML tags
# (3) Remove links
# (4) Remove stop words
# (5) Stem
# (6) Tokenize

import re
import json
import csv
import htmlentitydefs
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
stemmer = SnowballStemmer('english')


def strip_html_tags_and_links(text):
	return re.sub(r'(http\S+|<.*?>)', ' ', text)
	

def convert_html_entities(text):
	def fixup(m):
		text = m.group(0)
		
		if text[:2] == "&#":
			# character reference
			try:
				if text[:3] == "&#x":
					return unichr(int(text[3:-1], 16))
				else:
					return unichr(int(text[2:-1]))
			except ValueError:
				pass
		else:
			# named entity
			try:
				text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
			except KeyError:
				pass
		return text  # leave as is
	
	return re.sub("&#?\w+;", fixup, text)


def remove_non_alphanumeric(text):
	return re.sub('[^\w -]', ' ', text)


def stem(words):
	return [stemmer.stem(w) for w in words]


# Keeping here for now but unused at the moment.
def remove_stop_words(words):
	return [w for w in words if w not in stopwords.words('english')]
	

def clean(data):
	comments = [data['comment'], data['response']]
	
	cleaners = [
		strip_html_tags_and_links,
		convert_html_entities,
		remove_non_alphanumeric,
		word_tokenize,
		stem,
		remove_stop_words
	]
	
	for cleaner in cleaners:
		comments = [cleaner(comments[0]), cleaner(comments[1])]
	
	comment, response = comments
	
	return {
		'comment': ' '.join(comment).encode('utf-8'),
		'response': ' '.join(response).encode('utf-8')
	}


print 'Reading from raw JSON file...'

with open('comment_response_raw.json') as f:
	raw_data = json.loads(f.read())

print 'Cleaning data...'

clean_data = [clean(x) for x in raw_data]
	
print 'Writing cleaned data to JSON file...'

with open('comment_response_clean.json', 'w+') as f:
	f.write(json.dumps(clean_data, sort_keys=True, indent=2))
	f.close()

print 'Writing cleaned data to CSV file...'

headers = clean_data[0].keys()

with open('comment_response_clean.csv', 'w+') as f:
	dict_writer = csv.DictWriter(f, headers)
	dict_writer.writeheader()
	dict_writer.writerows(clean_data)
	f.close()
	
print 'Done.'