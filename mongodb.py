from pymongo import MongoClient
from path import *
from bson.json_util import loads

urlFabio = 'mongodb+srv://fabioluciani:WAn3fCWXuS54YG9R@sentimentanalysistwitte.kngytmy.mongodb.net/'
urlAndrea = "mongodb+srv://andreafancellu:51yikZeu5I1nAMzR@sentimentanalysistwitte.kngytmy.mongodb.net/"


def retrieve_data_from_json(path):
    with open(path, 'r', encoding='utf8') as f:
        return loads(f.read())

def insert_lex_res_on_db(db):
    path = f'{PREP_PATH}/lexResources.json'
    lex_res = retrieve_data_from_json(path)
    db.lexResources.insert_many(lex_res)

def main():
    client = MongoClient(urlAndrea)
    db = client.maadb_project
    insert_lex_res_on_db(db)
    print("inserted lexResources")

if __name__ == '__main__':
    main()