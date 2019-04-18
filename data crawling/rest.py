# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is for ReST crawling including duplicates removal

import sys
import json
import couchdb
import tweepy
from tweepy import OAuthHandler
days = []
# find location in json file.
def location(friend):
    locationString = friend.location
    locationString = friend.time_zone
    try:
        locationString = friend.status.place
    except Exception:
        pass
    return ''


# connect to twitter
try:
    consumer_key = 'WPOiyomOxUf0oB1n0mOGyyLFs'
    consumer_secret = '8sPNhBpzPDgefscDKU6bAXLzTNTiCyx7fgQs9x8zgyecrKz8cj'
    access_token = '886915377829105665-9Ed7FM0bMeSeReD9FePZYdOl2dURlXP'
    access_secret = '4BGkCVdANs20i5riW5kDbztKzVsC8z3eDogjGCnsKVb7j'
except KeyError:
    sys.stderr.write("TWITTER_* environment variables not set\n")
    sys.exit(1)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)


couch = couchdb.Server('http://:5984')
db = None

#create databacse
def creatDB(DBname):
    dbName = DBname
    try:
        db = couch[dbName]
    except couchdb.http.ResourceNotFound:
        try:
            couch.create(dbName)
            db = couch[dbName]
        except couchdb.http.PreconditionFailed:
            pass
    return db

def Day_DB(day,data):
    if day not in days:
        days.append(day)
        creatDB(day).save(data)



'''
this part is used to remove duplicate twitters

---------------------------------------------------------------------------

'''

#for key,value in doc.items():
#    dict[key] = value
dict = {}
records = []
doc = {}

def connect_to_couchdb(path):
    couch = couchdb.Server(path)
    return couch


def set_db(couch,db_name):
    try:
        db = couch[db_name]
    except couchdb.http.ResourceNotFound:
        try:
            couch.create(db_name)
            db = couch[db_name]
        except couchdb.http.PreconditionFailed:
            pass
    return db


def set_dict(db):
    # pre '0' string eusure this database-info-related record is always
    # displayed in the first place
    if '000SS1 RECORD' in db:
        doc = db['000SS1 RECORD']
    else:
        doc = {'_id': '000SS1 RECORD'}
        db.save(doc)

    dict = {}
    for key,value in doc.items():
        dict[key] = value
    return dict


def clear_records():
    records.clear()


def duplicate_remove(name, tweets):
    new_record_number = 0
    if name in dict.keys():
        end_id, start_id = dict[name].split()
        for tweet in tweets:
            if int(tweet['_id']) > int(end_id):
                records.append(tweet)
                new_record_number += 1
            else:
                dict[name] = (tweets[0]['_id'])+" "+start_id
                print(name+':'+dict[name])
                break
    else:
        for tweet in tweets:
            records.append(tweet)
        dict[name] = (tweets[0]['_id'])+" "+(tweets[len(tweets)-1]['_id'])
        new_record_number = len(tweets)
    return new_record_number


def write_to_couchdb(db):
    records.insert(0,dict)
    print(len(records))
    print(records[0])
    db.update(records)

    db['SS1 RECORD'] = dict

    clear_records()
    set_dict(db)


'''
this part is to crawl data by REST_API and the source file also is twitter
data which we got it from twitter by streaming_API.
------------------------------------------------------------------------------
'''


def get_twitters(db):

    with open('Canberra_out.json') as f:
        write_sign = 0
        end_sign = 0;

        for line in f:
            tweet = json.loads(line)
            name = tweet['user']['name']

            tweets_by_name = []
            for status in tweepy.Cursor(api.user_timeline, id=name).items():
                day = status.created_at.day
                tweet_temp = {'_id':status.id_str,'name':name,'time':day,'location':location(status.user),'text':status.text,'coordinates':status._json['coordinates']}
                tweets_by_name.append(tweet_temp)



            if len(tweets_by_name)!=0:
                new_record_number = duplicate_remove(name,tweets_by_name)
                write_sign += new_record_number

            #write_happens_each_100000pieces_info_received

            if(write_sign > 100000):
                write_to_couchdb(db)
                write_sign = 0;
                break

        #write leaving info
        end_sign = 1
        if end_sign:
            write_to_couchdb(db)

if __name__=="__main__":
    couch = connect_to_couchdb('http://127.0.0.1:5984/')
    db = set_db(couch,'ss1')
    dict = set_dict(db)

    get_twitters(db)
