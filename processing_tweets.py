from path import *
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag 
from collections import Counter
from bson.json_util import dumps
import os
from constants import *
from pymongo import MongoClient
import re
import multiprocessing


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


def retrive_lex_resource(db):
	lex = db.lexResourcesWords.find()
	for l in lex:
		lexical_resources[l["lemma"]] = l["resources"]

def find_resources_from_lexical_res(word):
	if word in lexical_resources.keys():
		return lexical_resources[word]
	else:
		return "null"
		
def remove_usr_url(tweet):
	return tweet.replace("USERNAME", "").replace("URL", "")

def remove_punctuation(tweet):
	return re.sub(r'[^\w\s]', " ", tweet) 

def tokenize_tweet(tweet):
	return tweet.strip().split(" ")

def find_hashtags(tweet):
	regex = '#\w+'
	hashtag_list = re.findall(regex, tweet)
	for ht in hashtag_list:
		tweet = tweet.replace(ht, "")
	return tweet, hashtag_list

def find_emoji(tweet_tokenized):
	#TODO find_emoji direttamente su stringhe
	new_tweet_tokenized = [] 
	list_of_emoji = []
	for word in tweet_tokenized:
		if word in emoji_pos or word in emoji_neg or word in others_emoji:
			list_of_emoji.append(word)
		else:
			new_tweet_tokenized.append(word)
	return new_tweet_tokenized, list_of_emoji

def find_emoticon(tweet_tokenized):
	#TODO find_emoticon direttamente su stringhe
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

def elaborate_tweet(tokenized_tweet, db):
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
def get_tweets(sentiment, db):
	
	final_list, hashtags, emojis, emoticons = [], [], [], []
	with open(TWEET_PATH.format(sentiment), 'r', encoding="utf8") as f:
		for i, line in enumerate(f.readlines()):
			#! Una volta fatto emoji ed emoticon su str spostarlo sopra punteggiatura
			tweet_dict = {}
			print(i)
			line = remove_usr_url(line)
			line, hashtags = find_hashtags(line)
			##! QUI
			line = remove_punctuation(line)
			
			line = tokenize_tweet(line)
			line, emojis = find_emoji(line)
			line, emoticons = find_emoticon(line)
			list_of_words = elaborate_tweet(line, db)
			tweet_dict["doc_number"] = i
			tweet_dict["words"] = list_of_words
			tweet_dict["emojis"] = emojis
			tweet_dict["emoticons"] = emoticons
			tweet_dict["hashtags"] = hashtags
			tweet_dict["sentiment"] = sentiment
			
			final_list.append(tweet_dict)
	
	file_path = os.path.join(PREP_PATH, f"preprocessed_tweets_{sentiment}.json")
	with open(file_path, 'w', encoding='utf8') as f:
		f.write(dumps(final_list))


def main():
	client = MongoClient(urlAndrea)
	db = client.maadb_project
	retrive_lex_resource(db)
	get_tweets("anger", db)
	'''n_processes = len(sentiments)
	processes = []
	for i in range(n_processes):
		print(i)
		print(sentiments[i])
		p = multiprocessing.Process(target=get_tweets, args=[sentiments[i], db])
		p.start()
		processes.append(p)
	for p in processes:
		p.join()
	'''
	print("Tweet json has been built correctly.")
	
if __name__ == "__main__":
	main() 