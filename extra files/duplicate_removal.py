# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used to remove the duplicates in 'geo_tweets.json' which was
# downloaded from the big collection provided by Professor Sinnott, and save
# to couchdb

import json
import couchdb

tweets = []
user_ids = {}

couch_db = couchdb.Server('http://admin:admin@115.146.85.180:5984/')
couch_db.create('geo_tag_tweets')
db = couch_db['geo_tag_tweets']


def robust_duplicate_remove(this_id):
    is_not_contained = True
    if len(user_ids) == 0:
        user_ids[this_id] = '1'
    elif not user_ids.__contains__(this_id):
        user_ids[this_id] = '1'
    else:
        is_not_contained = False

    return is_not_contained


def write_to_couchdb():
    db.update(tweets)
    tweets.clear()


with open('geo_tweets.json') as f:
    end_sign = 0

    for line in f:
        try:
            tweet = json.loads(line[:-2])
        except Exception:
            continue
        user_id = tweet['id']
        if robust_duplicate_remove(user_id):
            tweets.append(tweet)

        if len(tweets) >= 10000:
            write_to_couchdb()

    end_sign = 1

    if end_sign and len(tweets) != 0:
        write_to_couchdb()
