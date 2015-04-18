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


LIMIT = 10
NUM_PAGES_FOR_TAG = 5
UPDATE_ITERATION_PAUSE = 5*60  # in seconds


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
			start_time = datetime.now()
			cursor.execute(QUERY_TEMPLATE % (tag, 0, LIMIT * NUM_PAGES_FOR_TAG));
			all_rows = cursor.fetchall()
			for i in range(NUM_PAGES_FOR_TAG):
				offset = i * LIMIT
				redis_query_key = tag + ":" + str(offset) + ":" + str(LIMIT)
				rows_part = all_rows[i * LIMIT: (i + 1) * LIMIT]

				posts = []				
				for row in rows_part:
					posts.append(convertRowToPost(row))
				
				redis.set(redis_query_key, json.dumps(posts))
				print "Query \'" + redis_query_key + "\'" + " was updated. "
				
			end_time = datetime.now()
			delta = end_time - start_time
			output_str = "Total time elapsed for updating: \'" + tag + "\' "
			output_str += str(delta.total_seconds()) + " seconds"
			print output_str

		print "Pause between updates is started"
		time.sleep(UPDATE_ITERATION_PAUSE)
		print "Pause between updates is finished"



runScript()