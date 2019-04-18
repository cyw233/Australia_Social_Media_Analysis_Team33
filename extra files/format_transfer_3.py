# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used to transfer the format and add some attributes retrieved
# from AURIN on the json file of scenario 3 result on couchdb so it can be
# utilized more conveniently and efficiently on front end development.

import couchdb
import json

# create a new json file
couch = couchdb.Server('http://admin:admin@115.146.85.180:5984/')
couch.create('scenario3_new')
res_db = couch['scenario3_new']

# read the old json result and change the format, then save the new json into
# 'scenario1_new'
db = couch['scenario3_result']
all_tweets = db.view('_all_docs', include_docs=True)
suburb_list = ['North Melbourne', 'West Melbourne', 'East Melbourne',
               'Docklands', 'South Yarra', 'Southbank', 'Port Melbourne',
               'Melbourne', 'Carlton', 'Parkville']
suburb_pubs = {'North Melbourne': 5520, 'West Melbourne': 2767, 'East Melbourne': 7704,
               'Docklands': 22992, 'South Yarra': 832, 'Southbank': 20564,
               'Port Melbourne': 1195, 'Melbourne': 98889, 'Carlton': 17897, 'Parkville': 4079}
suburb_people = {'North Melbourne': 7498, 'West Melbourne': 2594, 'East Melbourne': 3095,
                 'Docklands': 6012, 'South Yarra': 2735, 'Southbank': 10447,
                 'Port Melbourne': 14521, 'Melbourne': 19430, 'Carlton': 10527, 'Parkville': 2529}
for tw in all_tweets:
    to_json = json.dumps(tw['doc'])
    tweet = json.loads(to_json)
    for sb in suburb_list:
        new_js = dict()
        new_js['suburb'] = sb
        new_js['total'] = tweet[sb]['total']
        new_js['amount'] = tweet[sb]['amount']
        new_js['pubs'] = suburb_pubs[sb]
        new_js['people'] = suburb_people[sb]

        try:
            new_js['avg_score'] = tweet[sb]['avg_score']
        except Exception:
            res_db.save(new_js)
            continue

        res_db.save(new_js)
