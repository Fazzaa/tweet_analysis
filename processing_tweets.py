from path import *
from nltk.stem import WordNetLemmatizer
from collections import Counter
from PIL import Image
import os
from constants import *
from pymongo import MongoClient

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

def get_negative_word():
	negative_words = [] 
	with open(NEG_WORDS_PATH, 'r', encoding="utf8") as f:
		line = f.readline()
		while line != '':
			negative_words.append(line)
			line = f.readline()

	return negative_words

def update_score(tweet, emotion):
	#print(emotion)
	#{parola: [scores]}
	#scores = [EMOSN, NRC, SS, ...]
	emosn, nrc, ss = [],[],[]
	result_dict = {}
	with open(os.path.join(PATH, f"{emotion}/EmoSN_{emotion.lower()}.txt")) as file:
			line = file.readline()
			while line != '':
				emosn.append(line.replace("\n", ""))
				line = file.readline()
	with open(os.path.join(PATH, f"{emotion}/NRC_{emotion.lower()}.txt")) as file:
		line = file.readline()
		while line != '':
			nrc.append(line.replace("\n", ""))
			line = file.readline()
	with open(os.path.join(PATH, f"{emotion}/sentisense_{emotion.lower()}.txt")) as file:
		line = file.readline()
		while line != '':
			ss.append(line.replace("\n", ""))
			line = file.readline()

	for word in tweet:
		score = []
		emotion_dict = main_dict[emotion]
		if word not in emotion_dict.keys():
			score.append(1 if word in emosn else 0)
			score.append(1 if word in nrc else 0)
			score.append(1 if word in ss else 0)
			result_dict[word] = score
	
	return result_dict

emotions = {
		   0: "Anger",
	       1: "Anticipation",
		   2: "Disgust",
		   3: "Fear",
		   4: "Joy",
		   5: "Sadness",
		   6: "Surprise",
		   7: "Trust"
		   }

anger_tweets = open(ANGER_PATH, encoding='utf-8')
anticipation_tweets = open(ANTICIPATION_PATH, encoding='utf-8')
disgust_tweets = open(DISGUST_PATH, encoding='utf-8')
fear_tweets = open(FEAR_PATH, encoding='utf-8')
joy_tweets = open(JOY_PATH, encoding='utf-8')
sadness_tweets = open(SADNESS_PATH, encoding='utf-8')
surprise_tweets = open(SURPRISE_PATH, encoding='utf-8')
trust_tweets = open(TRUST_PATH, encoding='utf-8')

list_of_file = [anger_tweets, anticipation_tweets, disgust_tweets, fear_tweets, joy_tweets, sadness_tweets, surprise_tweets, trust_tweets]

lm = WordNetLemmatizer()
hashtags = []
counters = []
emoji = []
emoticons = []

negative_words = get_negative_word()
main_dict = {
			"Anger": {},
	     	"Anticipation": {},
			"Disgust": {},
			"Fear": {},
			"Joy": {},
			"Sadness": {},
			"Surprise": {},
			"Trust": {}
			}
i = 0
while i in range(len(list_of_file)):
	file = list_of_file[i]
	c = Counter()
	for tweet in file.readlines():
		tweet = tweet.split(" ")

		tweet = [word.lower() for word in tweet if word not in remove_words and word not in punctuation]
		tweet = [slang_words[word] if word in slang_words.keys() else word for word in tweet]

		hashtags.append([word for word in tweet if word[0] == '#'])
		emoji.append([word for word in tweet if word in emoji_neg or word in emoji_pos or word in others_emoji])
		emoticons.append([word for word in tweet if word in neg_emoticons or word in pos_emoticons])

		tweet = [word for word in tweet if word not in emoji_neg and word not in emoji_pos and word not in pos_emoticons and word not in neg_emoticons and word not in others_emoji and word[0] != '#']
		
		tweet = [lm.lemmatize(word) for word in tweet]
		final_tweet = []
		next_word_negated = False
		
		for word in tweet:
			if word in negative_words:  
				next_word_negated = True  
			else:
				if word not in stop_words:
					if next_word_negated:
						final_tweet.append("not " + word)  
					else:
						final_tweet.append(word)

			next_word_negated = False
		
		dict_of_score = update_score(final_tweet, emotions[i])
		main_dict[emotions[i]].update(dict_of_score)
		c.update(final_tweet)
	
	print(c.most_common(20))
	print("Fine file")
	counters.append(c)
	i += 1
print("Fine di tutti i file")
	