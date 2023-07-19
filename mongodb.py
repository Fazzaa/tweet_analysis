from pymongo import MongoClient

urlFabio = 'mongodb+srv://fabioluciani:WAn3fCWXuS54YG9R@sentimentanalysistwitte.kngytmy.mongodb.net/'
urlAndrea = "mongodb+srv://andreafancellu:51yikZeu5I1nAMzR@sentimentanalysistwitte.kngytmy.mongodb.net/"

client = MongoClient(urlFabio)
print(client.list_database_names())
