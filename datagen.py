#!/usr/bin/python
#author vmykh

import MySQLdb
import string
import random

N_USERS = 500
N_POSTS = 10000

DB_HOST = 'localhost'
DB_USER = 'cache_tester'
DB_PASSWORD = 'very_secure_password'
DB_NAME = 'sait_example'

DB_START_ID = 1;


def generatePostTitle():
	MIN_TITLE_LEN = 10
	MAX_TITLE_LEN = 45
	titleLength = random.randint(MIN_TITLE_LEN, MAX_TITLE_LEN)
	title = random.choice(string.ascii_uppercase)
	titleLength -= 1
	title += ''.join(random.choice(string.ascii_lowercase + ' ') for i in range(titleLength))
	return title


def generateUserName():
	MIN_LETTERS_IN_UNAME = 3
	MAX_LETTERS_IN_UNAME = 12
	MIN_DIGITS_IN_UNAME = 0
	MAX_DIGITS_IN_UNAME = 4
	lettersInUName = random.randint(MIN_LETTERS_IN_UNAME, MAX_LETTERS_IN_UNAME)
	digitsInUName = random.randint(MIN_DIGITS_IN_UNAME, MAX_DIGITS_IN_UNAME)
	username = ''.join(random.choice(string.ascii_lowercase + '_') for i in range(lettersInUName))
	username += ''.join(random.choice(string.digits) for i in range(digitsInUName))
	return username


def generatePostContent():
	MIN_POST_LEN = 50
	MAX_POST_LEN = 300
	postLength = random.randint(MIN_POST_LEN, MAX_POST_LEN)
	post = random.choice(string.ascii_uppercase)
	postLength -= 1
	post += ''.join(random.choice(string.ascii_letters + '        .,-!?') for i in range(postLength))
	return post


def generateUsers():
	users = []
	for i in range(N_USERS):
		users.append(generateUserName())
	return users


def generateTags():
	tags = []
	tags.append('cats')
	tags.append('fun')
	tags.append('food')
	tags.append('starwars')
	tags.append('selfie')
	tags.append('insta')
	tags.append('lol')
	tags.append('iasa')
	tags.append('night')
	tags.append('photo')
	return tags


def generatePost(users, tags):
	MIN_TAGS_IN_POST = 1
	MAX_TAGS_IN_POST = 4
	MIN_RATES_OF_POST = 0;
	MAX_RATES_OF_POST = N_USERS / 10;

	MIN_SINGLE_RATE = 0
	MAX_SINGLE_RATE = 3

	post = {}
	post['title'] = generatePostTitle();
	post['content'] = generatePostContent();

	tag_ids = []
	tags_amount = random.randint(MIN_TAGS_IN_POST, MAX_TAGS_IN_POST + 1)
	for i in range(tags_amount):
		tag_id = random.randint(1, len(tags))
		# actual number of tags may be less than tags_amount
		if tag_id not in tag_ids:
			tag_ids.append(tag_id)
	post['tag_ids'] = tag_ids


	rates = []
	user_ids = []
	rates_amount = random.randint(MIN_RATES_OF_POST, MAX_RATES_OF_POST + 1)
	for i in range(rates_amount):
		user_id = random.randint(1, len(users))
		# actual number of users (and rates respectively) may be less than rates_amount
		if user_id not in user_ids:
			user_ids.append(user_id)

	for user_id in user_ids:
		rate = random.randint(MIN_SINGLE_RATE, MAX_SINGLE_RATE)
		rates.append({"user_id": user_id, "rate": rate})

	post['rates'] = rates;
	return post


def fillTagsInDB(connection, tags):
	cursor = connection.cursor()
	for i in range(len(tags)):
		cursor.execute("INSERT INTO tag VALUES (%d, '%s');" % (i + DB_START_ID, tags[i]))
	connection.commit()


def fillUsersInDB(connection, users):
	cursor = connection.cursor()
	for i in range(len(users)):
		cursor.execute("INSERT INTO user VALUES (%d, '%s');" % (i + DB_START_ID, users[i]))
	connection.commit()


def fillPostsInDB(connection, users, tags):
	PRINT_PERIOD = 50

	cursor = connection.cursor()
	
	for i in range(N_POSTS):
		post = generatePost(users, tags)
		query = "INSERT INTO post VALUES (%d, '%s', '%s');" % (i + DB_START_ID, post['title'], post['content'])
		cursor.execute(query)

		tag_ids = post['tag_ids']
		for tag_id in tag_ids:
			query = "INSERT INTO post_tag VALUES (%d, %d);" % (i + DB_START_ID, tag_id)
			cursor.execute(query)

		rates = post['rates']
		for single_rate in rates:
			query = "INSERT INTO post_rating VALUES"
			query += "(%d, %d, %d);" % (single_rate['user_id'], i + DB_START_ID, single_rate['rate'])
			cursor.execute(query)

		connection.commit()

		if(i % PRINT_PERIOD == 0):
			print str(i) + " posts created out of " + str(N_POSTS) + "  -  " + str(100 * float(i) / N_POSTS) + "%"


def getConnection():
	conn = MySQLdb.connect(host = DB_HOST, user = DB_USER, passwd = DB_PASSWORD, db = DB_NAME)
	return conn

def clearTables(connection):
	cursor = connection.cursor()
	cursor.execute("DELETE FROM post_rating")
	cursor.execute("DELETE FROM post_tag")
	cursor.execute("DELETE FROM post")
	cursor.execute("DELETE FROM user")
	cursor.execute("DELETE FROM tag")


def runScript():
	conn = getConnection()
	clearTables(conn)
	print "clearing tables finished"

	tags = generateTags()
	users = generateUsers()

	fillTagsInDB(conn, tags)
	fillUsersInDB(conn, users)
	fillPostsInDB(conn, users, tags)
	


runScript()


