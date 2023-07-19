from pymongo import MongoClient
from path import *
from bson.json_util import loads
from constants import urlAndrea, urlFabio


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

def main():
    client = MongoClient(urlAndrea)
    db = client.maadb_project
    insert_lex_res_on_db(db)
    print("inserted lexResources")

if __name__ == '__main__':
    main()