
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


def strip_html_tags(text):
	return re.sub(r'<.*?>', '', text)
	

def remove_links(text):
	return re.sub(r'https?:\/\/.*[\r\n]*', '', text)


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


def stem(words):
	return [stemmer.stem(w) for w in words]


# Keeping here for now but unused at the moment.
def remove_stop_words(words):
	unwanted = stopwords.words('english') + ['.', '!', '?', ';', ':']
	return [w for w in words if w not in unwanted]
	

def clean(data):
	comments = [data['comment'], data['response']]
	
	cleaners = [
		strip_html_tags,
		remove_links,
		convert_html_entities,
		word_tokenize,
		stem
	]
	
	for cleaner in cleaners:
		comments = [cleaner(comments[0]), cleaner(comments[1])]
	
	comment, response = comments
	
	return {
		'comment': ' '.join(comment),
		'response': ' '.join(response)
	}


with open('test_comments.json', 'r') as f:
	raw_data = json.loads(f.read())

clean_data = []

for comment_pair in raw_data:
	clean_data.append(clean(comment_pair))

with open('comment_response_clean.json', 'w+') as f:
	f.write(json.dumps(clean_data, sort_keys=True, indent=2))

headers = clean_data[0].keys()

with open('comment_response_clean.csv', 'w+') as f:
	dict_writer = csv.DictWriter(f, headers)
	dict_writer.writeheader()
	dict_writer.writerows(clean_data)