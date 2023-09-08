import os
from bson.json_util import dumps
from path import *

'''
Goal numero 1, per ogni risorsa:
{id: nome risorsa,
sentiment: nome sentimento,
totNumberWords: numero totale di parole per ogni file}

Goal numero 2,
{_id: ObjectId("..."),
 lemma : word,
 resources : [ {$ref : LexResources, $id : resource_filename}, {},... ]
}
{"lemma": "afraid", 
"resources": [{"resource": "sentisense", "sentiment": "anger"},
 			  {"resource": "sentisense", "sentiment": "disgust"}, 
			  {"resource": "NRC", "sentiment": "fear"}, 
			  {"resource": "LIWC", "sentiment": "neg"}, 
			  {"resource": "HL", "sentiment": "neg"}]}
'''

RESOURCES = {
	'anger': ['EmoSN', 'NRC', 'sentisense'],
	'anticipation': ['NRC', 'sentisense'],
	'disgust': ['NRC', 'sentisense'],
	'hate': ['sentisense'],
	'fear': ['NRC', 'sentisense'],
	'hope': ['sentisense'],
	'joy': ['EmoSN', 'NRC', 'sentisense'],
	'like': ['sentisense'],
	'love': ['sentisense'],
	'neg': ['GI', 'HL', 'ET', 'LIWC'],
	'pos': ['GI', 'HL', 'ET', 'LIWC'],
	'sadness': ['NRC', 'sentisense'],
	'surprise': ['NRC', 'sentisense'],
	'trust': ['NRC']
}

def retrive_information(sentiment):
	files = RESOURCES[sentiment]
	list_of_dict = []
	for file in files:
		complete_path = os.path.join(PATH, f'{sentiment.capitalize()}/{file}_{sentiment}.txt')
		with open(complete_path, 'r', encoding="utf8") as f: 
			n_Words = 0
			for line in f.readlines():
				if '_' not in line:
					n_Words += 1
		list_of_dict.append({
						'id': file, 
						'sentiment': sentiment, 
						'totNumberWords': n_Words
						})
	return list_of_dict

def retrive_word(sentiment):
	files = RESOURCES[sentiment]
	list_of_dict = []
	for file in files:
		complete_path = os.path.join(PATH, f'{sentiment.capitalize()}/{file}_{sentiment}.txt')
		with open(complete_path, 'r', encoding="utf8") as f: 
			for line in f.readlines():
				if '_' not in line:
					list_of_dict.append({'lemma': line.strip("\n"), 'resources': [{'resource': file, 'sentiment': sentiment}]}) 
	return list_of_dict

def merge_dicts(list_of_dicts):
	res_dict = []
	temp = []
	for d1 in list_of_dicts:
		if d1["lemma"] not in temp:
			for d2 in list_of_dicts:
				if (d1["lemma"] == d2["lemma"]) and d1["resources"] != d2["resources"]:
					d1["resources"].extend(d2["resources"])
			res_dict.append(d1)
			temp.append(d1["lemma"])
	return res_dict


def main():
	lex_res_word_list = []
	for sentiment in RESOURCES.keys():
		lex_res_word_list.extend(retrive_information(sentiment))
	
	file = os.path.join(PREP_PATH, "LexResources.json")
	with open(file, 'w', encoding='utf8') as f:
		f.write(dumps(lex_res_word_list, indent=2))
		print("Lexical Resources' Words json has been built correctly.")	

	lex_res_word_list.clear()
	for sentiment in RESOURCES.keys():
		lex_res_word_list.extend(retrive_word(sentiment))	
	print("Ciclo finito")
	list_final = merge_dicts(lex_res_word_list)
	

	file = os.path.join(PREP_PATH, "LexResourcesWords.json")
	with open(file, 'w', encoding='utf8') as f:
		f.write(dumps(list_final, indent=2))
		print("Lexical Word Resources json has been built correctly.")


if __name__== '__main__':
	main()