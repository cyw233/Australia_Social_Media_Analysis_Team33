# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used to upload the local json files (crawling from twitter using
# streaming) on VM to couchdb

import couchdb
import time
import json


couch = couchdb.Server('http://admin:admin@115.146.85.180:5984/')
# ensure the connection is successfully established
time.sleep(30)

file_list = ['Adelaide_out.json', 'Brisbane_out.json', 'Canberra_out.json',
             'Darwin_out.json', 'Hobart_out.json', 'Melbourne_out.json',
             'Perth_out.json', 'Sydney_out.json']

# save each json file into couchdb separately
for file in file_list:
    couch_filename = file[:-9].lower() + '_streaming'
    couch.create(couch_filename)
    res_db = couch[couch_filename]
    with open(file) as tw_file:
        for line in tw_file:
            try:
                tweet = json.loads(line[:-1])
            except Exception:
                continue

            res_db.save(tweet)
