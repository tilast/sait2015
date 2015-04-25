# sait2015 - conference

This repo is for demonstrating benefits of using cache and precomputations for web-services

To run presented programs you must have Redis and MySQL installed on your computer

First run script create_schema_sql in your MySQL client

Then fill this schema with data using script datagen.py (you may want to change some parameters like N_USERS, N_POSTS, DB_USER, etc)

Next you have to run in background redis_cache_update.py to start precomputations and caching

Having this done you can run actual testing scripts: mysql_fetch.py - for measuring query duration without using caching; and redis_fetch.py - measuring queries duration from precomputed cache
