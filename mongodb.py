from pymongo import MongoClient
from path import *
from bson.json_util import loads
from constants import urlAndrea, urlFabio, urlLocal

sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

def retrieve_data_from_json(path):
    with open(path, 'r', encoding='utf8') as f:
        return loads(f.read())

def insert_lex_res_on_db(db):
    path_1 = f'{PREP_PATH}/LexResources.json'
    path_2 = f'{PREP_PATH}/LexResourcesWords.json'
    lex_res = retrieve_data_from_json(path_1)
    lex_res_words = retrieve_data_from_json(path_2)
    db.lexResources.insert_many(lex_res)
    db.lexResourcesWords.insert_many(lex_res_words)

def insert_tweets_on_db(db):
    for sentiment in sentiments:
        path_1 = f'{PREP_PATH}/preprocessed_tweets_{sentiment}.json'
        tweets = retrieve_data_from_json(path_1)
        db.tweets.insert_many(tweets)

def main():
    client = MongoClient(urlLocal)
    db = client.maadb_project
    insert_lex_res_on_db(db)
    print("Inserted lexResources")
    #insert_tweets_on_db(db)
    print("Inserted tweets")

if __name__ == '__main__':
    main()