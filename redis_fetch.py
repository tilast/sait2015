#!/usr/bin/python
#author vmykh

import MySQLdb
import redis
import random
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

GET_ALL_TAGS_QUERY = ("SELECT name FROM tag;")


QUERIES_AMOUNT = 10
QUERY_OFFSET = 0
QUERY_LIMIT = 10;


def getMySQLConnection():
	conn = MySQLdb.connect(host = DB_HOST, user = DB_USER, passwd = DB_PASSWORD, db = DB_NAME)
	return conn

def getAllTags(conn):
	cursor = conn.cursor()
	cursor.execute(GET_ALL_TAGS_QUERY)
	rows = cursor.fetchall()
	tags = []
	for row in rows:
		tags.append(row[0])
	return tags

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
	conn = getMySQLConnection();
	cursor = conn.cursor()

	tags = getAllTags(conn)

	redis = getRedisConnection()

	print "Redis Fetch (With caching)\n\n\n"

	query_durations = [0.0] * QUERIES_AMOUNT

	for i in range(QUERIES_AMOUNT):
		current_tag = random.choice(tags)

		start_time = datetime.now()

		redis_query_key = current_tag + ":" + str(QUERY_OFFSET) + ":" + str(QUERY_LIMIT)
		posts_json = redis.get(redis_query_key)
		posts = json.loads(posts_json)

		end_time = datetime.now()
		delta = end_time - start_time
		sec = delta.total_seconds()
		query_durations[i] = sec

		for post in posts:
			printPost(post)
			print '\n-----------------\n'

		print ("Query #" + str(i) + " with tag \'" + current_tag + "\' finished."
				" Duration: " + str(delta.total_seconds()) + " sec\n\n\n\n\n")


	print "\n\nAverage query time: " + str(sum(query_durations) / float(QUERIES_AMOUNT))



runScript()