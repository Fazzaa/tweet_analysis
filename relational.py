import mysql.connector
from path import *
from mysql.connector import Error as MySqlError
from bson.json_util import loads
import os


PW = 'root'
sentiments = ['anger', 'anticipation', 'disgust', 'fear', 'hate', 'hope', 'joy', 'like', 'love', 'sadness', 'surprise', 'trust', 'pos', 'neg']

COMPLETED_OP = "Operazione riuscita con successo"

def retrieve_data_from_json(path):
	with open(path, 'r', encoding='utf8') as f:
		return loads(f.read())

def create_db_connection(host_name, user_name, user_password, db_name):
	connection = mysql.connector.connect(
		host=host_name,
		user=user_name,
		passwd=user_password,
		database=db_name
	)
	return connection

def exec_query(query, connection):
	print(query)
	cursor = connection.cursor()
	cursor.execute(query)
	connection.commit()
	
def create_table_sentiment(connection):
	#exec_query("drop table maadb_project.sentiment", connection)
	query = """CREATE TABLE `maadb_project`.`sentiment` (
				`nome` VARCHAR(20) NOT NULL, 
				PRIMARY KEY (`nome`));"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_lexical_resource(connection):
	#exec_query("drop table maadb_project.lexical_resource", connection)
	query = """CREATE TABLE `maadb_project`.`lexical_resource` (
  				`id_lex_res` INT NOT NULL AUTO_INCREMENT,
  				`sentiment` VARCHAR(20) NOT NULL,
  				`resource_name` VARCHAR(10) NOT NULL,
  				`number_of_words` INT NOT NULL,
  				PRIMARY KEY (`id_lex_res`),
  				CONSTRAINT `sentiment_lexres`
				FOREIGN KEY (`sentiment`) REFERENCES `maadb_project`.`sentiment` (`nome`)
				ON DELETE RESTRICT ON UPDATE CASCADE);
			"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_words(connection):
	#exec_query("drop table maadb_project.word", connection)
	query = """CREATE TABLE `maadb_project`.`word` (
				`id_word` INT NOT NULL AUTO_INCREMENT,
				`lemma` VARCHAR(20) NOT NULL,
				PRIMARY KEY (`id_word`));"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_token(connection):
	#exec_query("drop table maadb_project.token", connection)
	query = """CREATE TABLE `maadb_project`.`token` (
				`id_token` INT NOT NULL AUTO_INCREMENT,
				`token` VARCHAR(20) NOT NULL,
				`type` VARCHAR(20) NOT NULL,
				PRIMARY KEY (`id_token`));"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_tweet(connection):
	#exec_query("drop table maadb_project.tweet", connection)
	query = """CREATE TABLE `maadb_project`.`tweet` (
  				`id_tweet` INT NOT NULL AUTO_INCREMENT,
  				`sentiment` VARCHAR(20) NOT NULL,
  				`doc_number` INT NOT NULL,
  				PRIMARY KEY (`id_tweet`),
  				CONSTRAINT `sentiment_tweet`
				FOREIGN KEY (`sentiment`) REFERENCES `maadb_project`.`sentiment` (`nome`)
				ON DELETE RESTRICT ON UPDATE CASCADE);"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_wordsintweet(connection):
	#exec_query("drop table maadb_project.wordsintweet", connection)
	query = """CREATE TABLE `maadb_project`.`wordsintweet` (
  				`id_word` INT NOT NULL,
  				`id_tweet` INT NOT NULL,
  				`frequence` INT NOT NULL,
  				`pos_tag` VARCHAR(4) NULL,
  				PRIMARY KEY (`id_word`, `id_tweet`),
  				CONSTRAINT `tweet_id`
					FOREIGN KEY (`id_tweet`) REFERENCES `maadb_project`.`tweet` (`id_tweet`)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT `word_id`
					FOREIGN KEY (`id_word`) REFERENCES `maadb_project`.`word` (`id_word`)
					ON DELETE CASCADE ON UPDATE CASCADE);"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_tokenintweet(connection):
	#exec_query("drop table maadb_project.tokenintweet", connection)
	query = """CREATE TABLE `maadb_project`.`tokensintweet` (
  				`id_token` INT NOT NULL,
  				`id_tweet` INT NOT NULL,
  				`frequence` INT NOT NULL,
  				PRIMARY KEY (`id_token`, `id_tweet`),
  				CONSTRAINT `tweet_token_id`
					FOREIGN KEY (`id_tweet`) REFERENCES `maadb_project`.`tweet` (`id_tweet`)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT `token_id`
					FOREIGN KEY (`id_token`) REFERENCES `maadb_project`.`token` (`id_token`)
					ON DELETE CASCADE ON UPDATE CASCADE);"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def create_table_wordinlexres(connection):
	#exec_query("drop table maadb_project.wordsinlexres", connection)
	query = """CREATE TABLE `maadb_project`.`wordsinlexres` (
  				`id_word` INT NOT NULL,
  				`id_lexres` INT NOT NULL,
  				PRIMARY KEY (`id_word`, `id_lexres`),
				CONSTRAINT `id_word_lexres`
					FOREIGN KEY (`id_word`) REFERENCES `maadb_project`.`word` (`id_word`)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT `id_lexres`
					FOREIGN KEY (`id_lexres`) REFERENCES `maadb_project`.`lexical_resource` (`id_lex_res`)
					ON DELETE CASCADE ON UPDATE CASCADE);"""
	exec_query(query, connection)
	print(COMPLETED_OP)

def insert_sentiments(connection):
	query = """INSERT INTO `maadb_project`.`sentiment` (`nome`) 
				VALUES ('{}');"""
	for s in sentiments:
		exec_query(query.format(s), connection)
	print(COMPLETED_OP)

def insert_lexical_resources(connection, lex_res):
	query = """INSERT INTO `maadb_project`.`lexical_resource` (`sentiment`, `resource_name`, `number_of_words`)
				VALUES ('{}', '{}', '{}');"""
	for item in lex_res:
		exec_query(query.format(item["sentiment"], item["id"], item["totNumberWords"]), connection)
	print(COMPLETED_OP)

def insert_tweet(connection):
	insert = "('{}', '{}'),"
	for s in sentiments:
		query = """INSERT INTO `maadb_project`.`tweet` (`sentiment`, `doc_number`) VALUES"""
		for i in range(60000):
			query += insert.format(s, i)
		exec_query(query[:-1] + ";", connection)
		print(f"Completed {s}")
	print(COMPLETED_OP)

def create_db_tables(connection):
	create_table_sentiment(connection)
	create_table_lexical_resource(connection)
	create_table_words(connection)
	create_table_token(connection)
	create_table_tweet(connection)
	create_table_wordsintweet(connection)
	create_table_tokenintweet(connection)
	create_table_wordinlexres(connection)

def main():
	connection = create_db_connection("localhost", "root", PW, 'maadb_project')
	#create_db_tables(connection)
	#insert_sentiments(connection)
	#path_1 = f'{PREP_PATH}/LexResources.json'
	#lex_res = retrieve_data_from_json(path_1)
	#insert_lexical_resources(connection, lex_res)
	insert_tweet(connection)
if __name__=="__main__":
	main()