# Cluster and Cloud Computing Assignment 2
# Team 33
# Chenyang Gao, Chenyang Wang, Naijun Wang, Xiaoming Zhang, Yangyang Luo

# This file is for multi-thread crawling, which need the output of streaming
# crawling. For each tweet in streaming crawling output, it looks for all
# the followers' ID of that user

import json
import tweepy
import time
from tweepy import OAuthHandler
import datetime
import threading
import sys


# create a list to store all the keys we gonno use in crawling.
accounts = []

accounts.append({
'consumer_key' : 'bBAyCzuDVTwZ4a5cEdXZsCBtD',
'consumer_secret' : 'ULRqMIeXV4g5UZRsUYAutQLoA9KGc2LBgtzUm11J4DApoxIi1v',
'access_token' : '988328066442117121-ttz85CDp8aGRzs2AzmoeWAFJ1HQrFmq',
'access_secret' : 'ApDqhTbpsAOGoYnhQQqbLjlosHPUBzVKlxm1wS2PBG8Q7'})

accounts.append({
'consumer_key' : 'NzTjQ8zRqPwZ4cLSEEWUdVdjw',
'consumer_secret' : '5CVujT15x27zV9rWjRQnXKndzhv0X8Vs8f12gBce2yv5rvvYg6',
'access_token' : '757469703547879424-Kh4SPuIszS5H5KB6Y94rdVBEytD6dqZ',
'access_secret' : 'yo2FJonTg4MzDxfPEsNbxHePusYGUlvlfn1pM3Mnu389d'})

accounts.append({
'consumer_key' : 'JKzUa3F0yiNLq460kORxe0KM2',
'consumer_secret' : '1efpSOFxA0VF3IIOor16jIguTcOOIdO9dd8znDQRvE6fRQY4FT',
'access_token' : '757469703547879424-PQCqCVPhItFqIljX7PxSH6BDbyqg5Mh',
'access_secret' : 's146GWiClnBv7s2VOBsEMzZjTnNTj82LX9H9kWT8k7LDY'})

accounts.append({
'consumer_key' : '2pyIOUL88DZC0eJoB3g8rEi2c',
'consumer_secret' : 'YBVeCMvY52i7alwuyraEDfsPLZZS7DzsiH7I8OSvXdg2G0JrcR',
'access_token' : '990483629007491072-f07UmvW6ujV0RIkjjvkM9ztSl0hjHaU',
'access_secret' : 'UwPV29gRobeC4QKzJF4IAKVI0tIzrtgcgOs8CRA8lDB5Y'})

accounts.append({
'consumer_key' : 'ggj5065prOE3U9rDBlBHFgstv',
'consumer_secret' : 'fhBeZNpLA13JTQtLGcyUZRfTpcdQ52LEqBktM38ZvO9JODh5no',
'access_token' : '886915377829105665-MiqALJmL4lBWA6LjDkjeakxXyRehnow',
'access_secret' : 'sYAViam2W9FkONZF35CoPpYqwl151BWO65HkdAINeIdbj'})

accounts.append({
'consumer_key' : 'vIhYVId1iqF9OXe0nsm8PuQS7',
'consumer_secret' : 'EFc6HECXyH11AoZeDGnO0UBpzYtR2tubK25Iy6aGTi94UJANyM',
'access_token' : '757469703547879424-clpaRO4SevGYXNpNJadb2YCegtz3lkY',
'access_secret' : '9YrIkbIh5kR2Zu0jq1mAKBW5bisSHqz5AhJmCe1w1S2BM'})

accounts.append({
'consumer_key' : 'UJOUOdb8vrA2dyHEfzwDeufVx',
'consumer_secret' : 'DJVDZF92CBLzWww1a45FSzP4rxvIJghN8srppvuI9SsCjK4F8R',
'access_token' : '757469703547879424-ERJSqNGrxa8SKIINx8xzsZPMCKbRzYn',
'access_secret' : 'I67D4DCuAiNfiPycHRpiKQBLkEtGiAUXCVIl3KwT3ckGV'})

accounts.append({
'consumer_key' : '33upX85iOQJokSAitjeYdrLYe',
'consumer_secret' : 'KYzJ34ZDbU42cEHyDZoC0Rslei4fZDdeKcOo9yVM12BHaYEzTr',
'access_token' : '991811002051190784-Jv1RTELxwR84H7hhQKqhN73WqF5qeoi',
'access_secret' : 'TPivp5druHqJilyjrHGayQ4ruzwqnalssMbRl2WpKRbL2'})

accounts.append({
'consumer_key' : 'mPxoYZnw5TkDEZ7kuV8aw8pMn',
'consumer_secret' : 'f3fnZ8JAdkZLVaTj9RSolT6xS2mQVjAKJSbwxeMjXH7sxWVAY9',
'access_token' : '991811002051190784-ETw3fbPFOaXN6ts6ianpquus1XmvLW8',
'access_secret' : 'kfB3KiCOt3YmV1NvOJgLWIcxAPcGQGzMc7316WBZIZYGL'})

accounts.append({
'consumer_key' : 't6rFKIyGzL7ciFCHwBQ9kmW4C',
'consumer_secret' : 'vNos4WZwEeKuCJ9KcHZE0w1l9LVTioSBKnGtnYqVkkTVZwoYEA',
'access_token' : '991811002051190784-u7m62COYISts7RXXiIYBWy7cIS8Vc0n',
'access_secret' : 'kfB3KiCOt3YmV1NvOJgLWIcxAPcGQGzMc7316WBZIZYGL'})

accounts.append({
'consumer_key' : '7uf9G5wsuEbbPzNcCjdKhF5zU',
'consumer_secret' : 'NhBpgB9wI0tZmjCbpFCIArgdUVN9UxxEq63dl3KifxlS7BMr30',
'access_token' : '990483629007491072-GhTtxz6SqsNekF0OT95zgC0Cz2fKfzv',
'access_secret' : 'cLJzxmvKqTrKaF49NcVvUbSVSuTFvh4IiePuS1A2fJZ8E'})

accounts.append({
'consumer_key' : 'RTxrXZb25k4TayGJYYixoONIb',
'consumer_secret' : 'gzL3qLlwLucxjjjS1muxw5mO7gcQiI7VI5EXaxNBRmf5gWxVqc',
'access_token' : '990483629007491072-Ih9zkACSaRwR8xbvCnw2r6D1qG6WiQa',
'access_secret' : '5ew3Ql9L1myi7m9M2FxnaHNiJz4rENk1Iz9kBIiAFqDWg'})

accounts.append({
'consumer_key' : 'U8ujyUy7xu39Il4cJV6az2jLI',
'consumer_secret' : 'GsmDj5ceZnSur4ypNrnCEXbLR8PpI7HH7GabdUiwRnf0IIrt7c',
'access_token' : '990483629007491072-d9YeEMLvDZB7AvGHXbY0qvT6m4h1Ygd',
'access_secret' : 'pxlbXeq3RcsnlJx84JQ2dTSlKXUQ7tiEkP6c2vpTRYnUC'})

accounts.append({
'consumer_key' : 'UYbVXHko0nfcvz5TGEdt9Udbg',
'consumer_secret' : 'HcLLApNSz2eAk9df32Nknxn5ZNiN6vBNtB1MVEzCGs1pa1sYut',
'access_token' : '886915377829105665-OiF9lZQ7OXj6DuHNGB0T1CErsmsH7iM',
'access_secret' : '1uAytoX0ETdOvFmqaKFrDXOQej8mWPrEoSENnWw45GgFz'})

accounts.append({
'consumer_key' : '9jkA4TZaE85RW1ChGzs3NotrI',
'consumer_secret' : 'pDbJcjlcZ4hKT7n7jNT9YSRKPIVNf7aIegdzthGwtb1eg97nlc',
'access_token' : '988328066442117121-57t34wyyj2PmUP43KDe4Rpf6VZTqSvq',
'access_secret' : 'd3IvraqYRY12IWhU1xFuhzpLnz98B1pF4uy23QzOs6epl'})

accounts.append({
'consumer_key' : 'pgfsBn29xx7WjIeQw2FgUzOhu',
'consumer_secret' : '7wAY43hjOqDS6zx0LNci56uwzdq4mcLiokOhF2jEKkTuSMoG5v',
'access_token' : '988328066442117121-ZwhKFqZJLuq3mSSGjm9MAXVZNHMn7je',
'access_secret' : 'qFiPcqj4v33kAniIbdgSHEjI8yezi2vo3bnDw85XSdcbb'})

accounts.append({
'consumer_key' : 'kiLz9KGKoly1YqlFniL91Avcl',
'consumer_secret' : 'Ff7NxXzN9eUrHOWyWyjlscXwLSC3pUMwYBaVMmy37mOe6yNVUg',
'access_token' : '925304770193104897-UiszSjN3pO0faPZNyd1E6bwgEW68jJy',
'access_secret' : 'mqIOj8W9oYNSoqY5KrX1GmPGEXZkTq9IYSNsfsJVRS1k5'})

accounts.append({
'consumer_key' : 'UlcKpGAMU5fW9uHi1xmEHlfF1',
'consumer_secret' : 'sHRmEho9FwnOjHYzaNFj010DR0YyoCdW7Ino1l13L9EfeWCr52',
'access_token' : '925304770193104897-XMkXssOdzi2Olfw9cBA4YhCYFq4Nl4P',
'access_secret' : 'NR1OQYnZeM756wbfgXClNG3VCXKSBu8rTfD8UMK7uaeHR'})

accounts.append({
'consumer_key' : '3UeohllAkEkmHQxSAKBnKFbU6',
'consumer_secret' : 'HZZCJOJwEQeurIzHfblFeXkcT2BXCne4fKFyTx3FdZvTferoPf',
'access_token' : '925304770193104897-BAmZXEjwQIZNvfls0sakdGouFhMJC9l',
'access_secret' : 'MPLpKgXlgEdYkMH2MAkUM1Q6hWlLuUvQULOS6dvchbt66'})

accounts.append({
'consumer_key' : 'WPOiyomOxUf0oB1n0mOGyyLFs',
'consumer_secret' : '8sPNhBpzPDgefscDKU6bAXLzTNTiCyx7fgQs9x8zgyecrKz8cj',
'access_token' : '886915377829105665-9Ed7FM0bMeSeReD9FePZYdOl2dURlXP',
'access_secret' : '4BGkCVdANs20i5riW5kDbztKzVsC8z3eDogjGCnsKVb7j'})


twitternum = 0

# open the original data which got from Streaming method.
with open('Darwin_out.json') as f:
    for i in f:
        twitternum += 1

threadprocessnum = twitternum // 5

# in order to accelerate the rate of crawling data, we use 5 threads to crawl.
class myThread(threading.Thread):
    startnum = 0
    endnum = 0
    threadID = 0
    followercount = 0
    def __init__(self, startnum, endnum, threadID):
        threading.Thread.__init__(self)
        self.startnum = startnum
        self.endnum = endnum
        self.threadID = threadID
        print('threadID:',threadID,' start.')



    def run(self):
        apis = []
        apinum = 0
        for account in accounts[4 * (self.threadID - 1): 4 * self.threadID]:
            auth = OAuthHandler(account['consumer_key'], account['consumer_secret'])
            auth.set_access_token(account['access_token'], account['access_secret'])
            apis.append(tweepy.API(auth))
        count = 0
        with open('Darwin_out.json') as f:
            with open('Darwin_username.txt','a') as f2:
                with open('Darwin_userID.txt','a') as f3:
                    for line in f:
                        count += 1
                        if count >= self.startnum and count < self.endnum:
                            tweet = json.loads(line)
                            id = tweet['user']['id']
                            try:
                                for friend in tweepy.Cursor(apis[apinum].friends, id=id).items(200):
                                    if friend.lang != 'en':
                                        continue
                                    else:
                                        contents = json.dumps(friend._json)
                                        contents = json.loads(contents)
                                        # f2.write(jsonpickle.encode(friend._json, unpicklable=False) +'\n')
                                        f2.write(contents['name'])
                                        f2.write('\n')
                                        f3.write(str(contents['id']))
                                        f3.write('\n')
                                        # f2.write('\n')
                                        self.followercount += 1
                            # once rate error happens, we just change to another accounts so that can continue crawl
                            except tweepy.error.RateLimitError as e:
                                apinum = apinum + 1
                                apinum = apinum % 4
                                showtime = datetime.datetime.now()
                                time.sleep(40)
                                print('threadID:',self.threadID,showtime,apinum,self.followercount)
                            except tweepy.error.TweepError as e:
                                if e.response is None:
                                    raise e
                                # error rate limit exceeded
                                elif e.api_code == 326:
                                    apinum = apinum + 1
                                    apinum = apinum % 4
                                    time.sleep(60*5)
                                    print(e, apinum, 'friend e.api_code 326', sys._getframe().f_lineno,'threadID:',self.threadID)
                                # error Bad request or URI not found
                                elif e.response.status_code in set([401, 404]):
                                    print(e, "status_code 401 404", sys._getframe().f_lineno, 'threadID:',self.threadID)
                                    continue
                                else:
                                    print(e, 'friend', sys._getframe().f_lineno)
                                    continue

thread1 = myThread(0 ,threadprocessnum, 1)
thread2 = myThread(threadprocessnum*1, threadprocessnum*2, 2)
thread3 = myThread(threadprocessnum*2, threadprocessnum*3, 3)
thread4 = myThread(threadprocessnum*3, threadprocessnum*4, 4)
thread5 = myThread(threadprocessnum*4, threadprocessnum*5, 5)


thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
