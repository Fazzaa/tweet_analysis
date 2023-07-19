from path import *
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag 
from collections import Counter
from PIL import Image
import os
from constants import *
from pymongo import MongoClient
import re

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

TWEET_PATH = r"C:\Users\andre\Desktop\Progetti\ProgettoMAADBmateriale\Twitter_messaggi\dataset_dt_{}_60k.txt"
emotions = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]


def retrive_lex_resource(lexname):
	client = MongoClient(urlAndrea)
	db = client.maadb_project
	lex = db.lexResourcesWords.find({"lemma":lexname})
	for l in lex:
		return l["resources"]

def remove_usr_url(tweet):
	return tweet.replace("USERNAME", "").replace("URL", "")

def remove_punctuation(tweet):
	return re.sub(r'[^\w\s]', " ", tweet) 

def tokenize_tweet(tweet):
	return tweet.split(" ")

def find_hashtags(tweet_tokenized):
	#TODO farlo in modo da rimuovere hashtags da stringa direttamente 
	new_tweet_tokenized = [] 
	list_of_hashtags = []
	for word in tweet_tokenized:
		if word[0] == '#':
			list_of_hashtags.append(word)
		else:
			new_tweet_tokenized.append(word)
	return new_tweet_tokenized, list_of_hashtags

def find_emoji(tweet_tokenized):
	#TODO uguale hashtag
	new_tweet_tokenized = [] 
	list_of_emoji = []
	for word in tweet_tokenized:
		if word in emoji_pos or word in emoji_neg or word in others_emoji:
			list_of_emoji.append(word)
		else:
			new_tweet_tokenized.append(word)
	return new_tweet_tokenized, list_of_emoji

def find_emoticon(tweet_tokenized):
	#TODO uguale hashtag
	new_tweet_tokenized = [] 
	list_of_emoticon = []
	for word in tweet_tokenized:
		if word in pos_emoticons or word in neg_emoticons:
			list_of_emoticon.append(word)
		else:
			new_tweet_tokenized.append(word)
	return new_tweet_tokenized, list_of_emoticon

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
	for word in tokenized_tweet:
		word = convert_slang(word)
		word, tag = pos_tag(word)
		word = get_lemma(word)
		word = remove_stopwords(word)

def get_tweets():
	hashtags = []
	emojis = []
	emoticons = []
	for em in emotions:
		with open(TWEET_PATH.format(em), 'r', encoding="utf8") as f:
			for line in f.readlines():
				line = remove_usr_url(line)
				line = remove_punctuation(line)
				line = tokenize_tweet(line)
				line, hashtags = find_hashtags(line) 
				line, emojis = find_emoji(line) 
				line, emojticons = find_emoticon(line) 
				elaborate_tweet(line)


def main():
	#get_tweets()
	print(get_frequency("B", "A A A A A B B B D"))

if __name__ == "__main__":
	main() 