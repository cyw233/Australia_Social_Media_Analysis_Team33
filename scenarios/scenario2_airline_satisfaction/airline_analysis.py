# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is used for scenario 2 which is a satisfaction survey of 8
# (probably) popular airline corporations in Australia. Using keyword to
# determine whether a tweet is relevant, then using sentiment analysis method
# to get the satisfaction level. The tweets data is from the big collection
# provided by Professor Sinnott but we have removed the duplicates using our
# own way (see 'extra_files/duplicate_removal.py') and save to couchdb.

import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import couchdb
from replace_symbols import sentiment_score


# make a properties list for each corporation
def get_result():
    result = {
        'Jetstar': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Virgin': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Qantas': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Air New Zealand': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Singapore Airlines': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Qatar Airways': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Emirates': {'positive': 0, 'negative': 0, 'neg_rate': 0},
        'Hawaiian Airlines': {'positive': 0, 'negative': 0, 'neg_rate': 0}
    }
    return result


# return the sentiment score of each tweet
def attribute(tweet):
    analyzer = SentimentIntensityAnalyzer()
    score = sentiment_score(analyzer, tweet)
    return score['compound']


# for each tweet, if any word in a list is in the tweet content, the tweet is
# identified as related to some airline corporation, then use attribute()
# function to get the score. If the score is negative, the tweet is identified
# negative, otherwise it is positive
def match_tweet(tweet, result):
    airline_list = {
        'Jetstar': ['jetstar', 'jetstarairways', 'jetstartair'],
        'Virgin': ['virgin air', 'virgin australia', 'virgin airline', 'virgin au'],
        'Qantas': ['qantas'],
        'Air New Zealand': ['air new zealand', 'air nz', 'new zealand airline', 'new zealand airlines'],
        'Singapore Airlines': ['singapore airline', 'singapore air', 'singaporeairline', 'singaporeair', 'singapore airlines'],
        'Qatar Airways': ['qatar airways', 'qatar airway', 'qatar airline', 'qatar air', 'qatarairlines', 'qatar airlines'],
        'Emirates': ['emirates air', 'emirates airlines', 'emirates airline'],
        'Hawaiian Airlines': ['hawaiian airline', 'hawaii airline', 'hawaiian air', 'hawaii airline', 'hawaiian airlines']
    }
    try:
        text = tweet['text']
    except Exception:
        return result

    score = attribute(text)

    for al in airline_list:
        all_names = airline_list[al]
        for name in all_names:
            if name in text.lower():
                for res in result:
                    if res == al:
                        if score >= 0:
                            result[res]['positive'] += 1
                        else:
                            result[res]['negative'] += 1

                        result[res]['neg_rate'] = round(result[res]['negative']/(result[res]['negative']+result[res]['positive']), 4)

    return result


# retrieve tweets from couchdb
def airline_output():
    result = get_result()

    couch = couchdb.Server('http://admin:admin@115.146.85.180:5984/')

    db = couch['geo_tag_tweets']
    all_tweets = db.view('_all_docs', include_docs=True)
    for tw in all_tweets:
        to_json = json.dumps(tw['doc'])
        tweet = json.loads(to_json)

        new_tweet = dict()
        new_tweet['text'] = tweet['value']['properties']['text']
        result = match_tweet(new_tweet, result)

    # return result
    couch.create('scenario2_new')
    res_db = couch['scenario2_new']

    for airline in result:
        new_js = dict()
        new_js['airline'] = airline
        new_js['rate'] = result[airline]
        res_db.save(new_js)


airline_output()
