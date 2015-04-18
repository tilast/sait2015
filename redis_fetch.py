#!/usr/bin/python
#author vmykh

import MySQLdb
import redis
import simplejson as json
from datetime import datetime

DB_HOST = 'localhost'
DB_USER = 'cache_tester'
DB_PASSWORD = 'very_secure_password'
DB_NAME = 'sait_example'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

QUERY_TEMPLATE = ("SELECT post.id, post.title, post.content, SUM(post_rating.rate) as total_rate "
					"FROM post "
					"JOIN post_tag ON post_tag.post_id=post.id "
					"JOIN tag ON post_tag.tag_id=tag.id "
					"JOIN post_rating ON post_rating.post_id=post.id "
					"WHERE tag.name='%s' "
					"GROUP BY post.id "
					"ORDER BY total_rate DESC "
					"LIMIT %d;")

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

def printPost(post):
	print "Post ID: " + str(post["id"])
	print "Title: " + post["title"]
	print "Total Rate: " + str(post["total_rate"])
	print "-----"
	print "Content:"
	print post["content"]

def getRedisConnection():
	conn = redis.StrictRedis(host = REDIS_HOST, port = REDIS_PORT, db = REDIS_DB)
	return conn


def runScript():
	# conn = getMySQLConnection();
	# cursor = conn.cursor()

	redis = getRedisConnection()

	tag = "iasa"
	limit = 50

	start_time = datetime.now()
	# cursor.execute(QUERY_TEMPLATE % (tag, limit));
	

	redis_query_key = tag + ":" + str(limit) 

	posts_json = redis.get(redis_query_key)

	end_time = datetime.now()
	delta = end_time - start_time

	posts = json.loads(posts_json)
	# rows = cursor.fetchall()
	for post in posts:
		printPost(post)
		print '\n-----------------\n'

	# redis.set(redis_query_key, json.dumps(posts))

	

	print "time elapsed for fetching from redis: " + str(delta.total_seconds() * 1000) + "ms"


runScript()