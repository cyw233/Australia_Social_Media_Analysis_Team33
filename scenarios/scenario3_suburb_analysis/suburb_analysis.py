# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used for scenario 3 which is to find out the total sentiment
# score and the number of tweets in each suburb of Melbourne. After getting
# the result, we will analyze the relationship between the number of tweets
# and different indexes for each suburb collected from the AURIN.  The tweets
# data is from the big collection provided by Professor Sinnott but we have
# removed the duplicates using our own way
# (see 'extra_files/duplicate_removal.py') and save to couchdb.

import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from replace_symbols import sentiment_score
import couchdb


# This function is used to calculate the suburb and sentiment score of a tweet.
def locate_suburb():
    sub_coor = {'North Melbourne': ['-37.8022012', '-37.8000914', '144.9416029', '144.9419838'],
                'West Melbourne': ['-37.8133083', '-37.8123573', '144.9496298', '144.9510905'],
                'East Melbourne': ['-37.8213329', '-37.8185147', '144.9809162', '144.9854959'],
                'Docklands': ['-37.8202581', '-37.8199099', '144.9401479', '144.9408225'],
                'South Yarra': ['-37.8378434', '-37.8342358', '144.9826127', '144.983353'],
                'Southbank': ['-37.8239465', '-37.8218246', '144.9574779', '144.9608441'],
                'Port Melbourne': ['-37.8349607', '-37.8345359', '144.9435057', '144.9440406'],
                'Melbourne': ['-37.8174466', '-37.8170813', '144.9534728', '144.9539332'],
                'Carlton': ['-37.7976399', '-37.7974399', '144.9671651', '144.9673651'],
                'Parkville': ['-37.8029348', '-37.8027348', '144.9589354', '144.9591354'],
    }

    # total: total compound score, amount: the number of tweets
    suburbs = {
               'North Melbourne': {'total': 0, 'amount': 0},
               'West Melbourne': {'total': 0, 'amount': 0},
               'East Melbourne': {'total': 0, 'amount': 0},
               'Docklands': {'total': 0, 'amount': 0},
               'South Yarra': {'total': 0, 'amount': 0},
               'Southbank': {'total': 0, 'amount': 0},
               'Port Melbourne': {'total': 0, 'amount': 0},
               'Melbourne': {'total': 0, 'amount': 0},
               'Carlton': {'total': 0, 'amount': 0},
               'Parkville': {'total': 0, 'amount': 0}
    }

    analyzer = SentimentIntensityAnalyzer()

    # retrieve data from couchdb
    couch = couchdb.Server('http://admin:admin@115.146.85.180:5984/')

    db = couch['geo_tag_tweets']
    all_tweets = db.view('_all_docs', include_docs=True)
    for tw in all_tweets:
        to_json = json.dumps(tw['doc'])
        tweet = json.loads(to_json)

        new_tweet = dict()
        new_tweet['location'] = tweet['value']['properties']['location']
        new_tweet['text'] = tweet['value']['properties']['text']
        new_tweet['coordinates'] = tweet['value']['geometry']['coordinates']

        lat = new_tweet['coordinates'][1]
        lon = new_tweet['coordinates'][0]
        for suburb in sub_coor:
            if float(sub_coor[suburb][0]) <= lat <= float(sub_coor[suburb][1]):
                if float(sub_coor[suburb][2]) <= lon <= float(
                        sub_coor[suburb][3]):
                    new_tweet['suburb'] = suburb
                    score = sentiment_score(analyzer, new_tweet['text'])
                    new_tweet['score'] = score['compound']
                    suburbs = suburb_stat(suburbs, new_tweet)

    couch.create('scenario3_result')
    res_db = couch['scenario3_result']
    res_db.save(suburbs)


# calculate the total sentiment score of each suburb, and when the number of
# tweets in a suburb is bigger than 10, calculate the average score
def suburb_stat(suburbs, tweet):
    try:
        suburb = tweet['suburb']
    except Exception:
        return

    suburbs[suburb]['total'] += tweet['score']
    suburbs[suburb]['amount'] += 1

    for suburb in suburbs:
        # calculate the average sentiment score if the number of tweets in this
        # suburb is over 10
        if suburbs[suburb]['amount'] >= 10:
            avg_score = suburbs[suburb]['total'] / suburbs[suburb]['amount']
            suburbs[suburb]['avg_score'] = avg_score

    return suburbs


locate_suburb()
