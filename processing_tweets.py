from path import *
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag 
from collections import Counter
from bson.json_util import dumps
import os
from constants import *
from pymongo import MongoClient
import re
from cleantext import clean
import emoji


'''
{"doc_number": 0, 
"words": [{"lemma": "randomly", "pos_tag": "RB", "freq": 1, "in_lex_resources": [{"resource": "NRC", "sentiment": "surprise"}, {"resource": "HL", "sentiment": "neg"}]}, 
		{"lemma": "got", "pos_tag": "VBD", "freq": 1, "in_lex_resources": null}, 
		{"lemma": "really", "pos_tag": "RB", "freq": 1, "in_lex_resources": [{"resource": "EmoSN", "sentiment": "joy"}]}, 
		{"lemma": "hot", "pos_tag": "JJ", "freq": 1, "in_lex_resources": [{"resource": "NRC", "sentiment": "anger"}, {"resource": "EmoSN", "sentiment": "anger"}, {"resource": "HL", "sentiment": "pos"}]}], 
"emojis": ["\ud83d\ude25", "\ud83d\ude09"], 
"emoticons": [], 
"hashtags": ["#sotakeoffallyourclothes", "#noimalady", "#unlessitsgirlsnight"], 
"sentiment": "trust"},
'''
lexical_resources = {}
sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]


def retrive_lex_resource():
	client = MongoClient(urlLocal)
	db = client.maadb_project
	lex = db.lexResourcesWords.find()
	for l in lex:
		lexical_resources[l["lemma"]] = l["resources"]

def find_resources_from_lexical_res(word):
	if word in lexical_resources.keys():
		return lexical_resources[word]
	else:
		return []
		
def remove_usr_url(tweet):
	return tweet.replace("USERNAME", "").replace("URL", "")

def remove_punctuation(tweet):
	return re.sub(r'[^\w\s]', " ", tweet) 

def find_hashtags(tweet):
	regex = '#\w+'
	hashtag_list = re.findall(regex, tweet)
	for ht in hashtag_list:
		tweet = tweet.replace(ht, "")
	return tweet, hashtag_list

def find_emoji(tweet):
	emoji_pattern = re.compile(emoji.get_emoji_regexp())
	emoji_list = emoji_pattern.findall(tweet)
	tweet_cleaned = clean(tweet, no_emoji=True)
	return tweet_cleaned, emoji_list

def find_emoticon(tweet):
	list_of_emoticon = []

	for ep in pos_emoticons:
		if ep in tweet:
			list_of_emoticon.append(ep)
			tweet = tweet.replace(ep, "")
	for en in neg_emoticons:
		if en in tweet:
			list_of_emoticon.append(en)
			tweet = tweet.replace(en, "")

	return tweet, list_of_emoticon 
		
def tokenize_tweet(tweet):
	return tweet.strip().split(" ")

def remove_stopwords(word):
	return word if word not in stop_words else ""

def convert_slang(word):
	return word if word not in slang_words else slang_words[word]

def get_lemma(word):
	lemmatizer = WordNetLemmatizer()
	return lemmatizer.lemmatize(word)

def get_frequency(word, tokenized_tweet):
	c = Counter(tokenized_tweet)
	return c[word]

def elaborate_tweet(tokenized_tweet):
	tag = None 
	tweet = []
	for word in tokenized_tweet:
		lemma_dict = {}
		word = convert_slang(word)
		word, tag = pos_tag([word])[0]
		word = get_lemma(word)
		word = remove_stopwords(word)
		if word != "":
			freq = get_frequency(word, tokenized_tweet)
			resources = find_resources_from_lexical_res(word)
			lemma_dict["lemma"] = word
			lemma_dict["pos_tag"] = tag
			lemma_dict["freq"] = freq
			lemma_dict["in_lex_resources"] = resources
			tweet.append(lemma_dict)
	return tweet

def get_tweets(sentiment):
	final_list, hashtags, emojis, emoticons = [], [], [], []
	with open(TWEET_PATH.format(sentiment), 'r', encoding="utf8") as f:
		for i, line in enumerate(f.readlines()):
			tweet_dict = {}
			print(i)
			line = remove_usr_url(line)
			line, hashtags = find_hashtags(line)
			line, emojis = find_emoji(line)
			line, emoticons = find_emoticon(line)
			line = remove_punctuation(line)
			
			line = tokenize_tweet(line)
			list_of_words = elaborate_tweet(line)
			tweet_dict["doc_number"] = i
			tweet_dict["words"] = list_of_words
			tweet_dict["emojis"] = emojis
			tweet_dict["emoticons"] = emoticons
			tweet_dict["hashtags"] = hashtags
			tweet_dict["sentiment"] = sentiment
			
			final_list.append(tweet_dict)
	
	file_path = os.path.join(PREP_PATH, f"preprocessed_tweets_{sentiment}.json")
	with open(file_path, 'w', encoding='utf8') as f:
		f.write(dumps(final_list, indent=2))


def main():
	retrive_lex_resource()
	n_processes = len(sentiments)
	processes = []
	for i in range(n_processes):
		get_tweets(sentiments[i])
	print("Tweet json has been built correctly.")
	
if __name__ == "__main__":
	main() 