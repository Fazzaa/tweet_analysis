from path import *
from wordcloud import WordCloud
from nltk.stem import PorterStemmer, WordNetLemmatizer
from collections import Counter
import matplotlib.pyplot as plt

anger_tweets = open(ANGER_PATH, encoding='utf-8')
anticipation_tweets = open(ANTICIPATION_PATH, encoding='utf-8')
disgust_tweets = open(DISGUST_PATH, encoding='utf-8')
fear_tweets = open(FEAR_PATH, encoding='utf-8')
joy_tweets = open(JOY_PATH, encoding='utf-8')
sadness_tweets = open(SADNESS_PATH, encoding='utf-8')
surprise_tweets = open(SURPRISE_PATH, encoding='utf-8')
trust_tweets = open(TRUST_PATH, encoding='utf-8')

list_of_file = [anger_tweets, anticipation_tweets, disgust_tweets, fear_tweets, joy_tweets, sadness_tweets, surprise_tweets, trust_tweets]

punctuation = [",", "?", "!", ".", ";", ":", "\\",
			   "/", "(", ")", "&", "_", "+", "=", "<", ">", "\n"]

pos_emoticons = ['B-)', ':)', ':-)', ":')", ":'-)", ':D', ':-D', ':\'-)', ":')", ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}', ':^)', '8-D',
				 '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D', '=-3', '=3', 'B^D', ':-))', ':*', ':^*', '( \'}{\' )', '^^', '(^_^)', '^-^', "^.^", "^3\^", "\^L\^", "<3"]

neg_emoticons = [':(', ':-(', ":'(", ":'-(", '>:[', ':-c', ':c', ':-<', ':<', ':-[', ':[', ':{', ':\'-(', ':\'(', ' _( ', ':\'[', "='(", "' [", "='[",
				 ":'-<", ":' <", ":'<", "=' <", "='<", "T_T", "T.T", "(T_T)", "y_y", "y.y", "(Y_Y)", ";-;", ";_;", ";.;", ":_:", "o .__. o", ".-.", "</3"]

emoji_pos = [u'\U0001F601', u'\U0001F602', u'\U0001F603', u'\U0001F604', u'\U0001F605', u'\U0001F606', u'\U0001F609', u'\U0001F60A', u'\U0001F60B',
			 u'\U0001F60E', u'\U0001F60D', u'\U0001F618', u'\U0001F617', u'\U0001F619', u'\U0001F61A', u'\U0000263A', u'\U0001F642', u'\U0001F917',
			 u'\U0001F607', u'\U0001F60F', u'\U0001F61C', u'\U0001F608', u'\U0001F646', u'\U0001F48F', u'\U0001F44C', u'\U0001F44F', u'\U0001F48B',
			 u'\U0001F638', u'\U0001F639', u'\U0001F63A', u'\U0001F63B', u'\U0001F63C', u'\U0001F63D', u'\U0001F192', u'\U0001F197']

emoji_neg = [u'\U0001F625', u'\U0001F60C', u'\U00002639', u'\U0001F641', u'\U0001F612', u'\U0001F614', u'\U0001F615', u'\U0001F616', u'\U0001F632',
			 u'\U0001F61E', u'\U0001F61F', u'\U0001F622', u'\U0001F62D', u'\U0001F626', u'\U0001F627', u'\U0001F628', u'\U0001F631', u'\U0001F621',
			 u'\U0001F620', u'\U0001F64D', u'\U0001F64E', u'\U0000270A', u'\U0001F44A', u'\U0001F494', u'\U0001F4A2', u'\U0001F5EF', u'\U0001F63E',
			 u'\U0001F63F']

others_emoji = [u'\U0001F004', u'\U0001F0CF', u'\U0001F300', u'\U0001F301', u'\U0001F302', u'\U0001F303', u'\U0001F304', u'\U0001F305', u'\U0001F306', u'\U0001F307',
				u'\U0001F309', u'\U0001F30A', u'\U0001F30B', u'\U0001F30F', u'\U0001F313', u'\U0001F315', u'\U0001F31B', u'\U0001F320', u'\U0001F330', u'\U0001F331',
				u'\U0001F334', u'\U0001F337', u'\U0001F338', u'\U0001F339', u'\U0001F33A', u'\U0001F33B', u'\U0001F33C', u'\U0001F33D', u'\U0001F33E', u'\U0001F33F',
				u'\U0001F340', u'\U0001F341', u'\U0001F342', u'\U0001F343', u'\U0001F344', u'\U0001F345', u'\U0001F346', u'\U0001F347', u'\U0001F348', u'\U0001F349',
				u'\U0001F34C', u'\U0001F34D', u'\U0001F34E', u'\U0001F34F', u'\U0001F351', u'\U0001F352', u'\U0001F353', u'\U0001F355', u'\U0001F356', u'\U0001F357',
				u'\U0001F358', u'\U0001F35A', u'\U0001F35B', u'\U0001F35C', u'\U0001F35D', u'\U0001F35E', u'\U0001F35F', u'\U0001F360', u'\U0001F361', u'\U0001F362',
				u'\U0001F363', u'\U0001F364', u'\U0001F366', u'\U0001F367', u'\U0001F368', u'\U0001F369', u'\U0001F36A', u'\U0001F36B', u'\U0001F36C', u'\U0001F36D',
				u'\U0001F36E', u'\U0001F36F', u'\U0001F371', u'\U0001F372', u'\U0001F373', u'\U0001F374', u'\U0001F375', u'\U0001F376', u'\U0001F377', u'\U0001F378',
				u'\U0001F37A', u'\U0001F37B', u'\U0001F380', u'\U0001F381', u'\U0001F382', u'\U0001F384', u'\U0001F385', u'\U0001F386', u'\U0001F387', u'\U0001F388',
				u'\U0001F389', u'\U0001F38A', u'\U0001F38B', u'\U0001F38C', u'\U0001F38D', u'\U0001F38E', u'\U0001F38F', u'\U0001F390', u'\U0001F391', u'\U0001F392',
				u'\U0001F393', u'\U0001F3A0', u'\U0001F3A1', u'\U0001F3A2', u'\U0001F3A3', u'\U0001F3A4', u'\U0001F3A5', u'\U0001F3A6', u'\U0001F3A7', u'\U0001F3A8',
				u'\U0001F3A9', u'\U0001F3AA', u'\U0001F3AB', u'\U0001F3AC', u'\U0001F3AD', u'\U0001F3AE', u'\U0001F3AF', u'\U0001F3B0', u'\U0001F3B1', u'\U0001F3B2',
				u'\U0001F3B3', u'\U0001F3B4', u'\U0001F3B5', u'\U0001F3B6', u'\U0001F3B7', u'\U0001F3B8', u'\U0001F3B9', u'\U0001F3BA', u'\U0001F3BB', u'\U0001F3BC',
				u'\U0001F3BD', u'\U0001F3BE', u'\U0001F3BF', u'\U0001F3C0', u'\U0001F3C1', u'\U0001F3C2', u'\U0001F3C3', u'\U0001F3C4', u'\U0001F3C6', u'\U0001F3C8',
				u'\U0001F3CA', u'\U0001F3E0', u'\U0001F3E1', u'\U0001F3E2', u'\U0001F3E3', u'\U0001F3E5', u'\U0001F3E6', u'\U0001F3E7', u'\U0001F3E8', u'\U0001F3E9',
				u'\U0001F3EA', u'\U0001F3EB', u'\U0001F3EC', u'\U0001F3ED', u'\U0001F3EE', u'\U0001F3EF', u'\U0001F3F0', u'\U0001F40C', u'\U0001F40D', u'\U0001F40E',
				u'\U0001F411', u'\U0001F412', u'\U0001F414', u'\U0001F417', u'\U0001F418', u'\U0001F419', u'\U0001F41A', u'\U0001F41B', u'\U0001F41C', u'\U0001F41D',
				u'\U0001F41E', u'\U0001F41F', u'\U0001F420', u'\U0001F421', u'\U0001F422', u'\U0001F423', u'\U0001F424', u'\U0001F425', u'\U0001F426', u'\U0001F427',
				u'\U0001F428', u'\U0001F429', u'\U0001F42B', u'\U0001F42C', u'\U0001F42D', u'\U0001F42E', u'\U0001F42F', u'\U0001F430', u'\U0001F431', u'\U0001F432',
				u'\U0001F433', u'\U0001F434', u'\U0001F435', u'\U0001F436', u'\U0001F437', u'\U0001F438', u'\U0001F439', u'\U0001F43A', u'\U0001F43B', u'\U0001F43C',
				u'\U0001F43D', u'\U0001F43E', u'\U0001F440', u'\U0001F442', u'\U0001F443', u'\U0001F444', u'\U0001F445', u'\U0001F446', u'\U0001F447', u'\U0001F448',
				u'\U0001F449', u'\U0001F44A', u'\U0001F44B', u'\U0001F44C', u'\U0001F44D', u'\U0001F44E', u'\U0001F44F', u'\U0001F450', u'\U0001F451', u'\U0001F452',
				u'\U0001F453', u'\U0001F454', u'\U0001F455', u'\U0001F456', u'\U0001F457', u'\U0001F458', u'\U0001F459', u'\U0001F45A', u'\U0001F45B', u'\U0001F45C',
				u'\U0001F45D', u'\U0001F45E', u'\U0001F45F', u'\U0001F460', u'\U0001F461', u'\U0001F462', u'\U0001F463', u'\U0001F464', u'\U0001F466', u'\U0001F467',
				u'\U0001F468', u'\U0001F469', u'\U0001F46A', u'\U0001F46B', u'\U0001F46E', u'\U0001F46F', u'\U0001F470', u'\U0001F471', u'\U0001F472', u'\U0001F473',
				u'\U0001F474', u'\U0001F475', u'\U0001F476', u'\U0001F477', u'\U0001F478', u'\U0001F479', u'\U0001F47A', u'\U0001F47B', u'\U0001F47C', u'\U0001F47D',
				u'\U0001F47E', u'\U0001F47F', u'\U0001F480', u'\U0001F481', u'\U0001F482', u'\U0001F483', u'\U0001F484', u'\U0001F485', u'\U0001F486', u'\U0001F487',
				u'\U0001F488', u'\U0001F489', u'\U0001F48A', u'\U0001F48B', u'\U0001F48C', u'\U0001F48D', u'\U0001F48E', u'\U0001F48F', u'\U0001F490', u'\U0001F491',
				u'\U0001F492', u'\U0001F493', u'\U0001F494', u'\U0001F495', u'\U0001F496', u'\U0001F497', u'\U0001F498', u'\U0001F499', u'\U0001F49A', u'\U0001F49B',
				u'\U0001F49C', u'\U0001F49D', u'\U0001F49E', u'\U0001F49F', u'\U0001F4A0', u'\U0001F4A1', u'\U0001F4A2', u'\U0001F4A3', u'\U0001F4A4', u'\U0001F4A5',
				u'\U0001F4A6', u'\U0001F4A7', u'\U0001F4A8', u'\U0001F4A9', u'\U0001F4AA', u'\U0001F4AB', u'\U0001F4AC', u'\U0001F4AE', u'\U0001F4AF', u'\U0001F4B0',
				u'\U0001F4B1', u'\U0001F4B2', u'\U0001F4B3', u'\U0001F4B4', u'\U0001F4B5', u'\U0001F4B8', u'\U0001F4B9', u'\U0001F4BA', u'\U0001F4BB', u'\U0001F4BC',
				u'\U0001F4BD', u'\U0001F4BE', u'\U0001F4BF', u'\U0001F4C0', u'\U0001F4C1', u'\U0001F4C2', u'\U0001F4C3', u'\U0001F4C4', u'\U0001F4C5', u'\U0001F4C6',
				u'\U0001F4C7', u'\U0001F4C8', u'\U0001F4C9', u'\U0001F4CA', u'\U0001F4CB', u'\U0001F4CC', u'\U0001F4CD', u'\U0001F4CE', u'\U0001F4CF', u'\U0001F4D0',
				u'\U0001F4D1', u'\U0001F4D2', u'\U0001F4D3', u'\U0001F4D4', u'\U0001F4D5', u'\U0001F4D6', u'\U0001F4D7', u'\U0001F4D8', u'\U0001F4D9', u'\U0001F4DA',
				u'\U0001F4DB', u'\U0001F4DC', u'\U0001F4DD', u'\U0001F4DE', u'\U0001F4DF', u'\U0001F4E0', u'\U0001F4E1', u'\U0001F4E2', u'\U0001F4E3', u'\U0001F4E4',
				u'\U0001F4E5', u'\U0001F4E6', u'\U0001F4E7', u'\U0001F4E8', u'\U0001F4E9', u'\U0001F4EA', u'\U0001F4EB', u'\U0001F4EE', u'\U0001F4F0', u'\U0001F4F1',
				u'\U0001F4F2', u'\U0001F4F3', u'\U0001F4F4', u'\U0001F4F6', u'\U0001F4F7', u'\U0001F4F9', u'\U0001F4FA', u'\U0001F4FB', u'\U0001F4FC', u'\U0001F503',
				u'\U0001F50A', u'\U0001F50B', u'\U0001F50C', u'\U0001F50D', u'\U0001F50E', u'\U0001F50F', u'\U0001F510', u'\U0001F511', u'\U0001F512', u'\U0001F513',
				u'\U0001F514', u'\U0001F516', u'\U0001F517', u'\U0001F518', u'\U0001F519', u'\U0001F51A', u'\U0001F51B', u'\U0001F51C', u'\U0001F51D', u'\U0001F51E',
				u'\U0001F51F', u'\U0001F520', u'\U0001F521', u'\U0001F522', u'\U0001F523', u'\U0001F524', u'\U0001F525', u'\U0001F526', u'\U0001F527', u'\U0001F528',
				u'\U0001F529', u'\U0001F52A', u'\U0001F52B', u'\U0001F52E', u'\U0001F52F', u'\U0001F530', u'\U0001F531', u'\U0001F532', u'\U0001F533', u'\U0001F534',
				u'\U0001F535', u'\U0001F536', u'\U0001F537', u'\U0001F538', u'\U0001F539', u'\U0001F53A', u'\U0001F53B', u'\U0001F53C', u'\U0001F53D', u'\U0001F550',
				u'\U0001F551', u'\U0001F552', u'\U0001F553', u'\U0001F554', u'\U0001F555', u'\U0001F556', u'\U0001F557', u'\U0001F558', u'\U0001F559', u'\U0001F55A',
				u'\U0001F55B', u'\U0001F5FB', u'\U0001F5FC', u'\U0001F5FD', u'\U0001F5FE', u'\U0001F5FF', u'\U0001F601', u'\U0001F602', u'\U0001F603', u'\U0001F604',
				u'\U0001F605', u'\U0001F606', u'\U0001F609', u'\U0001F60F', u'\U0001F612', u'\U0001F613', u'\U0001F61C', u'\U0001F61D', u'\U0001F61E', u'\U0001F620',
				u'\U0001F621', u'\U0001F622', u'\U0001F623', u'\U0001F624', u'\U0001F625', u'\U0001F628', u'\U0001F629', u'\U0001F62A', u'\U0001F62B', u'\U0001F630',
				u'\U0001F631', u'\U0001F632', u'\U0001F633', u'\U0001F635', u'\U0001F637', u'\U0001F638', u'\U0001F639', u'\U0001F63A', u'\U0001F63B', u'\U0001F63C',
				u'\U0001F63D', u'\U0001F63E', u'\U0001F63F', u'\U0001F640', u'\U0001F645', u'\U0001F646', u'\U0001F647', u'\U0001F648', u'\U0001F649', u'\U0001F64A',
				u'\U0001F64B', u'\U0001F64C', u'\U0001F64E', u'\U0001F64F', u'\U0001F64F']

# additional_emoji=[u'\U+203C',u'\U+2049', u'\U+231A',u'\U+231B',u'\U+2600',u'\U+2601',u'\U+260E',u'\U+2611',u'\U+2614',u'\U+2615',u'\U+261D',u'\U+2648',u'\U+2648',
# u'\U+2649',u'\U+264A',u'\U+264B',u'\U+264C',u'\U+264D',u'\U+264E',u'\U+264F',u'\U+2650',u'\U+2651',u'\U+2652',u'\U+2653',u'\U+2660',u'\U+2663',u'\U+2665',u'\U+2666',
# u'\U+2668',u'\U+267B',u'\U+267F',u'\U+2693',u'\U+26A0',u'\U+26A1',u'\U+26AA',u'\U+26AB',u'\U+26BD',u'\U+26BE',u'\U+26C4',u'\U+26C5',u'\U+26CE',u'\U+26D4',u'\U+26EA',
# u'\U+26F2',u'\U+26F3',u'\U+26F5',u'\U+26FA',u'\U+26FD',u'\U+2934',u'\U+2935',u'\U+2934',u'\U+2B05',u'\U+2B06',u'\U+2B07',u'\U+2B50',u'\U+2B55',u'\U+2B50']

slang_words = {'afaik': 'as far as i know',
			   'afk': 'away from keyboard',
			   'asap': 'as soon as possible',
			   'atk': 'at the keyboard',
			   'atm': 'at the moment',
			   'a3': 'anytime, anywhere, anyplace',
			   'bak': 'back at keyboard',
			   'bbl': 'be back later',
			   'bbs': 'be back soon',
			   'bf': 'boyfriend',
			   'bfn/b4n': 'bye for now',
			   'brb': 'be right back',
			   'brt': 'be right there',
			   'btw': 'by the way',
			   'b4n': 'bye for now',
			   'cu': 'see you',
			   'cul8r': 'see you later',
			   'cya': 'see you',
			   'faq': 'frequently asked questions',
			   'fc': 'fingers crossed',
			   'fwiw': 'for what it\'s worth',
			   'fyi': 'for your information',
			   'gal': 'get a life',
			   'gf': 'girlfriend',
			   'gg': 'good game',
			   'gmta': 'great minds think alike',
			   'gr8': 'great!',
			   'g9': 'genius',
			   'ic': 'i see',
			   'icq': 'i seek you',
			   'idk': 'i don\'t know',
			   'ilu': 'i love you',
			   'imho': 'in my honest opinion',
			   'imo': 'in my opinion',
			   'iow': 'in other words',
			   'irl': 'in real life',
			   'kiss': 'keep it simple, stupid',
			   'ldr': 'long distance relationship',
			   'lmao': 'laugh my ass off',
			   'lol': 'laughing out loud',
			   'ltns': 'long time no see',
			   'l8r': 'later',
			   'mte': 'my thoughts exactly',
			   'm8': 'mate',
			   'nrn': 'no reply necessary',
			   'oic': 'oh i see',
			   'pita': 'pain in the ass',
			   'prt': 'party',
			   'prw': 'parents are watching',
			   'qpsa?': 'que pasa?',
			   'rofl': 'rolling on the floor laughing',
			   'roflol': 'rolling on the floor laughing out loud',
			   'rotflmao': 'rolling on the floor laughing my ass off',
			   'sk8': 'skate',
			   'stats': 'your sex and age',
			   'asl': 'age, sex, location',
			   'thx': 'thank you',
			   'ttfn': 'ta-ta for now!',
			   'ttyl': 'talk to you later',
			   'u': 'you',
			   'u2': 'you too',
			   'u4e': 'yours for ever',
			   'wb': 'welcome back',
			   'wtf': 'what the fuck',
			   'wtg': 'way to go!',
			   'wuf': 'where are you from?',
			   'w8': 'wait...',
			   '7k': 'sick',
			   '@' : 'at',
			   'i\'m': 'i am'
			   }

remove_words = ['USERNAME', 'URL']

ps = PorterStemmer()
lm = WordNetLemmatizer()

hashtags = []
counters = []
stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 
              'you', 'you\'re', 'you\'ve', 'you\'ll', 'you\'d', 'your', 'yours', 
              'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 
              'she\'s', 'her', 'hers', 'herself', 'it', 'it\'s', 'its', 'itself', 
              'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 
              'who', 'whom', 'this', 'that', 'that\'ll', 'these', 'those', 'am', 
              'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 
              'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 
              'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 
              'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 
              'through', 'during', 'before', 'after', 'above', 'below', 'to', 
              'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 
              'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 
              'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
              'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 
              'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 
              'don\'t', 'should', 'should\'ve', 'now', 'd', 'll', 'm', 'o', 're', 
              've', 'y', 'ain', 'aren', 'aren\'t', 'couldn', 'couldn\'t', 'didn', 'didn\'t', 
              'doesn', 'doesn\'t', 'hadn', 'hadn\'t', 'hasn', 'hasn\'t', 'haven', 'haven\'t', 
              'isn', 'isn\'t', 'ma', 'mightn', 'mightn\'t', 'mustn', 'mustn\'t', 'needn', 
              'needn\'t', 'shan', 'shan\'t', 'shouldn', 'shouldn\'t', 'wasn', 'wasn\'t', 
              'weren', 'weren\'t', 'won', 'won\'t', 'wouldn', 'wouldn\'t']

for file in list_of_file:
	c = Counter()
	for tweet in file.readlines():
		tweet = tweet.split(" ")

		tweet = [word.lower() for word in tweet if word not in remove_words and word not in punctuation]
		hashtags.append([word for word in tweet if word[0] == '#'])
		#TODO: trattamento emoji e emoticons
		
		tweet = [slang_words[word] if word in slang_words.keys() else word for word in tweet]
		
		tweet = [word for word in tweet if word not in stop_words]
		
		#stemming
		tweet = [ps.stem(word) for word in tweet]


		c.update(tweet)
	print("Fine file")
	counters.append(c)
	wc = WordCloud(width = 800, height = 800,background_color ='white').generate_from_frequencies(c)
	plt.figure(figsize = (8, 8), facecolor = None)
	plt.imshow(wc)
	plt.savefig("test.png")
	plt.axis("off")
	plt.tight_layout(pad = 0)
	
	plt.show()

print("Fine di tutti i file")
