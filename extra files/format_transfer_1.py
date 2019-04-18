# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used to transfer the format of scenario 1 result on couchdb so
# it can be utilized more conveniently and efficiently on front end
# development.

import couchdb
import json

# create a new json file
couch = couchdb.Server('http://admin:admin@115.146.85.180:5984/')
couch.create('scenario1_new')
res_db = couch['scenario1_new']

# read the old json result and change the format, then save the new json into
# 'scenario1_new' on couchdb
db = couch['scenario1_result_new']
all_tweets = db.view('_all_docs', include_docs=True)
for tw in all_tweets:
    to_json = json.dumps(tw['doc'])
    tweet = json.loads(to_json)
    ts = ['0-6', '6-12', '12-18', '18-24']
    new_js = dict()

    if tweet['_id'] == "d2446a1a94fded0c68a276e6a1035731":
        new_js['city'] = 'Canberra'
        for t in ts:
            tweet['Canberra'][t]['pos_rate'] = round(tweet['Canberra'][t]['positive'] / tweet['Canberra'][t]['amount'], 4) * 100
            tweet['Canberra'][t]['neg_rate'] = round(tweet['Canberra'][t]['negative'] / tweet['Canberra'][t]['amount'], 4) * 100

        new_js['GDP'] = 92400
        new_js['midnight'] = tweet['Canberra']['0-6']
        new_js['morning'] = tweet['Canberra']['6-12']
        new_js['afternoon'] = tweet['Canberra']['12-18']
        new_js['night'] = tweet['Canberra']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a1035ede":
        new_js['city'] = 'Darwin'
        for t in ts:
            tweet['Darwin'][t]['pos_rate'] = round(tweet['Darwin'][t]['positive'] / tweet['Darwin'][t]['amount'], 4) * 100
            tweet['Darwin'][t]['neg_rate'] = round(tweet['Darwin'][t]['negative'] / tweet['Darwin'][t]['amount'], 4) * 100

        new_js['GDP'] = 103800
        new_js['midnight'] = tweet['Darwin']['0-6']
        new_js['morning'] = tweet['Darwin']['6-12']
        new_js['afternoon'] = tweet['Darwin']['12-18']
        new_js['night'] = tweet['Darwin']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a1035fb4":
        new_js['city'] = 'Hobart'
        for t in ts:
            tweet['Hobart'][t]['pos_rate'] = round(tweet['Hobart'][t]['positive'] / tweet['Hobart'][t]['amount'], 4) * 100
            tweet['Hobart'][t]['neg_rate'] = round(tweet['Hobart'][t]['negative'] / tweet['Hobart'][t]['amount'], 4) * 100

        new_js['GDP'] = 55100
        new_js['midnight'] = tweet['Hobart']['0-6']
        new_js['morning'] = tweet['Hobart']['6-12']
        new_js['afternoon'] = tweet['Hobart']['12-18']
        new_js['night'] = tweet['Hobart']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a103639d":
        new_js['city'] = 'Melbourne'
        for t in ts:
            tweet['Melbourne'][t]['pos_rate'] = round(tweet['Melbourne'][t]['positive'] / tweet['Melbourne'][t]['amount'], 4) * 100
            tweet['Melbourne'][t]['neg_rate'] = round(tweet['Melbourne'][t]['negative'] / tweet['Melbourne'][t]['amount'], 4) * 100

        new_js['GDP'] = 66800
        new_js['midnight'] = tweet['Melbourne']['0-6']
        new_js['morning'] = tweet['Melbourne']['6-12']
        new_js['afternoon'] = tweet['Melbourne']['12-18']
        new_js['night'] = tweet['Melbourne']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a1036478":
        new_js['city'] = 'Perth'
        for t in ts:
            tweet['Perth'][t]['pos_rate'] = round(tweet['Perth'][t]['positive'] / tweet['Perth'][t]['amount'], 4) * 100
            tweet['Perth'][t]['neg_rate'] = round(tweet['Perth'][t]['negative'] / tweet['Perth'][t]['amount'], 4) * 100

        new_js['GDP'] = 69000
        new_js['midnight'] = tweet['Perth']['0-6']
        new_js['morning'] = tweet['Perth']['6-12']
        new_js['afternoon'] = tweet['Perth']['12-18']
        new_js['night'] = tweet['Perth']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a103740b":
        new_js['city'] = 'Sydney'
        for t in ts:
            tweet['Sydney'][t]['pos_rate'] = round(tweet['Sydney'][t]['positive'] / tweet['Sydney'][t]['amount'], 4) * 100
            tweet['Sydney'][t]['neg_rate'] = round(tweet['Sydney'][t]['negative'] / tweet['Sydney'][t]['amount'], 4) * 100

        new_js['GDP'] = 81300
        new_js['midnight'] = tweet['Sydney']['0-6']
        new_js['morning'] = tweet['Sydney']['6-12']
        new_js['afternoon'] = tweet['Sydney']['12-18']
        new_js['night'] = tweet['Sydney']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a1038004":
        new_js['city'] = 'Adelaide'
        for t in ts:
            tweet['Adelaide'][t]['pos_rate'] = round(tweet['Adelaide'][t]['positive'] / tweet['Adelaide'][t]['amount'], 4) * 100
            tweet['Adelaide'][t]['neg_rate'] = round(tweet['Adelaide'][t]['negative'] / tweet['Adelaide'][t]['amount'], 4) * 100

        new_js['GDP'] = 58400
        new_js['midnight'] = tweet['Adelaide']['0-6']
        new_js['morning'] = tweet['Adelaide']['6-12']
        new_js['afternoon'] = tweet['Adelaide']['12-18']
        new_js['night'] = tweet['Adelaide']['18-24']
    elif tweet['_id'] == "d2446a1a94fded0c68a276e6a1038ea8":
        new_js['city'] = 'Brisbane'
        for t in ts:
            tweet['Brisbane'][t]['pos_rate'] = round(tweet['Brisbane'][t]['positive'] / tweet['Brisbane'][t]['amount'], 4) * 100
            tweet['Brisbane'][t]['neg_rate'] = round(tweet['Brisbane'][t]['negative'] / tweet['Brisbane'][t]['amount'], 4) * 100

        new_js['GDP'] = 65400
        new_js['midnight'] = tweet['Brisbane']['0-6']
        new_js['morning'] = tweet['Brisbane']['6-12']
        new_js['afternoon'] = tweet['Brisbane']['12-18']
        new_js['night'] = tweet['Brisbane']['18-24']

    res_db.save(new_js)
