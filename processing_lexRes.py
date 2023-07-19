import os
from bson.json_util import dumps
from path import *
'''
Nostro goal, per ogni risorsa
{id: nome risorsa,
sentiment: nome sentimento,
totNumberWords: numero totale di parole per ogni file}
'''
base_path = '/home/fazza/Desktop/materiale_maadb/risorse_lessicali'

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
		complete_path = os.path.join(base_path, f'{sentiment.capitalize()}/{file}_{sentiment}.txt')
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

def main():
	lex_res_word_list = []
	for sentiment in RESOURCES.keys():
		lex_res_word_list.extend(retrive_information(sentiment))
	
	file = os.path.join(PREP_PATH, "LexResources.json")
	with open(file, 'w', encoding='utf8') as f:
		f.write(dumps(lex_res_word_list, indent=2))
		print("Lexical Resources' Words json has been built correctly.")	


if __name__== '__main__':
	main()