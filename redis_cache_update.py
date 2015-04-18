#!/usr/bin/python
#author vmykh

import MySQLdb
import redis
import simplejson as json
from datetime import datetime
import time

DB_HOST = 'localhost'
DB_USER = 'cache_tester'
DB_PASSWORD = 'very_secure_password'
DB_NAME = 'sait_example'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0


LIMIT = 50
NUM_PAGES = 5
UPDATE_ITERATION_PAUSE = 5  # in seconds


QUERY_TEMPLATE = ("SELECT post.id, post.title, post.content, SUM(post_rating.rate) as total_rate "
					"FROM post "
					"JOIN post_tag ON post_tag.post_id=post.id "
					"JOIN tag ON post_tag.tag_id=tag.id "
					"JOIN post_rating ON post_rating.post_id=post.id "
					"WHERE tag.name='%s' "
					"GROUP BY post.id "
					"ORDER BY total_rate DESC "
					"LIMIT %d, %d;")

GET_ALL_TAGS_QUERY = ("SELECT name FROM tag;")

#row sequence: id, title, content, total_rate
def convertRowToPost(row):
	post = {}
	post["id"] = row[0];
	post["title"] = row[1];
	post["content"] = row[2];
	post["total_rate"] = row[3]
	return post;


def convertPostToJson(post):
	return json.dump(post)


def getMySQLConnection():
	conn = MySQLdb.connect(host = DB_HOST, user = DB_USER, passwd = DB_PASSWORD, db = DB_NAME)
	return conn

def getRedisConnection():
	conn = redis.StrictRedis(host = REDIS_HOST, port = REDIS_PORT, db = REDIS_DB)
	return conn

def getAllTags(conn):
	cursor = conn.cursor()
	cursor.execute(GET_ALL_TAGS_QUERY)
	rows = cursor.fetchall()
	tags = []
	for row in rows:
		tags.append(row[0])
	return tags


def runScript():
	mysqlConn = getMySQLConnection();
	cursor = mysqlConn.cursor()

	redis = getRedisConnection()

	while(True):
		tags = getAllTags(mysqlConn)
		for tag in tags:
			for i in range(NUM_PAGES):
				offset = i * LIMIT

				start_time = datetime.now()

				cursor.execute(QUERY_TEMPLATE % (tag, offset, LIMIT));
				redis_query_key = tag + ":" + str(offset) + ":" + str(LIMIT)

				posts = []
				rows = cursor.fetchall()
				for row in rows:
					posts.append(convertRowToPost(row))
				
				redis.set(redis_query_key, json.dumps(posts))

				end_time = datetime.now()
				delta = end_time - start_time
				output_str = "Query \'" + redis_query_key + "\'" + " was updated. "
				output_str += "Time elapsed for operation: "
				output_str += str(delta.total_seconds() * 1000) + "ms"
				print output_str

		time.sleep(UPDATE_ITERATION_PAUSE)



runScript()