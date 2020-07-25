# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 22:29:10 2020

@author: Anirudh Raghavan
"""

import tweepy as tp
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from time import sleep, time
from vaderSentiment.vaderSentiment \
import SentimentIntensityAnalyzer
import os
import json
import boto3
from io import StringIO


abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Twitter Authentication

with open('authToken.json') as json_file:
    authToken = json.load(json_file)
    auth = tp.OAuthHandler(authToken['consumer_key'], authToken['consumer_secret'])
    auth.set_access_token(authToken['access_token'], authToken['access_token_secret'])

api = tp.API(auth, wait_on_rate_limit=True)

# Setup sentiment analyzer
analyser = SentimentIntensityAnalyzer()

def sentiment_analysis(text):
        score  = analyser.polarity_scores(text)["compound"]
        return score

# Defining keywords

listofkeywords = ['Economy','Recession','Stagnation','US GDP','Deflation','NYSE','NASDAQ','SP500',
                  'Government spending','US Deficit','Federal Reserve','Interest Rates','Dollar value',
                  'Inflation','CPI','Non-farm payroll','Industrial output',
                  'Manufacturing level', 'US Consumer Spending']


search_term = str()
for i in range(len(listofkeywords)):
    if i == 0:
        search_term += '"' + listofkeywords[i] + '"'
    else:
        search_term += ' ' + 'OR' + ' ' + '"' + listofkeywords[i] + '"'

search_term = search_term + ' -filter:retweets'

# Import csv file and extract key information

Econ_Tweets  = pd.read_csv("Sentiment_Tweets.csv", index_col = False)

start_tweet_count = Econ_Tweets.shape[0] # Number of tweets currently analyzed

current_time = datetime.now(timezone('UTC')).replace(tzinfo= None) # Current time of scraping
    
# Identify tweets that are greater than 24 hours or remove atleast 500 tweets

Tweet_times = list(Econ_Tweets['Tweet Time'])

times = [i for i in range(len(Tweet_times)) if 
         (current_time - datetime.strptime(Tweet_times[0],'%Y-%m-%d %H:%M:%S')).total_seconds()/3600 > 24]

ids_s = Econ_Tweets['id'].nsmallest(500+len(times))

ids_ind = [Econ_Tweets.index[Econ_Tweets['id'] == i][0] for i in ids_s] 

if len(times) < 500:
    for i in ids_ind:
        if i not in times:
            times.append(i)
        if len(times) == 500:
            break
    
Econ_Tweets = Econ_Tweets.drop(times) 

# Tweet main details
total_tweet_count = Econ_Tweets.shape[0] # Number of tweets currently analyzed



if len(Econ_Tweets['id']) > 0:
    sinceId = max([int(id) if id != np.nan else 0 for id in Econ_Tweets['id']])# The most recent tweet scraped
    maxId = min([int(id)  if id != np.nan else 0 for id in Econ_Tweets['id']]) # The oldest tweet scrpated

else:
    sinceId = 0
    maxId = 0 

# Identify number of tweets to be extracted

Obj_Tweets = 10000
maxTweets = Obj_Tweets-total_tweet_count

tweetCount = 0
twitter_runs = 0

start_time = time()

while tweetCount < maxTweets:
        
    tweet_reqd = min(100, maxTweets-tweetCount)
        
    if (sinceId == 0):
        public_tweets = api.search(q = search_term, count = tweet_reqd, lang = "en", 
                                   geocode = "39.809879,-98.556732,1340mi", result_type = "Popular")
        
        print("1")   
    else:
        public_tweets = api.search(q = search_term, count = tweet_reqd, lang = "en", 
                                   geocode = "39.809879,-98.556732,1340mi", result_type = "Popular", 
                                   since_id = sinceId+1)
        
        print("2")
        
        if len(public_tweets) == 0:
            public_tweets = api.search(q = search_term, count = tweet_reqd, lang = "en", 
                                       geocode = "39.809879,-98.556732,1340mi", result_type = "Popular", 
                                       max_id = maxId-1)
            
            print("3")
            
            check_time = [tweet.id for tweet in public_tweets if 
                       (current_time - tweet.created_at).total_seconds()/3600 <= 24]
            
            if len(check_time) == 0:
                signal = "No More Tweets Available"
                break
            
        else:
            check_time = [tweet.id for tweet in public_tweets if 
                       (current_time - tweet.created_at).total_seconds()/3600 <= 24]
        
            if len(check_time) == 0:
                signal = "No More Tweets Available"
                break
           
    twitter_runs += 1
    
    for tweet in public_tweets:
        
        if (current_time - tweet.created_at).total_seconds()/3600 <= 24:
        
            tweetCount += 1
            text = tweet.text
            user_name = tweet.user.name
            posted_time = tweet.created_at
            rt_count = tweet.retweet_count
            fav_count = tweet.favorite_count
            T_id = tweet.id
            sent_score = sentiment_analysis(text)
            
            Econ_Tweets = Econ_Tweets.append({'User Name' : user_name, 'Tweet' : text, 
                                                      'Tweet Time' : posted_time, 'RTs count' : rt_count, 
                                                      'Likes count' : fav_count, 'id' : T_id, 
                                                      'Sentiment Score' : sent_score}, ignore_index=True)
        
    if len(Econ_Tweets['id']) > 0:
        sinceId = max([int(id) if id != np.nan else 0 for id in Econ_Tweets['id']])# The most recent tweet scraped
        maxId = min([int(id)  if id != np.nan else 0 for id in Econ_Tweets['id']]) # The oldest tweet scrpated

    else:
        sinceId = 0
        maxId = 0  
    
    sleep(4)
                    
    elapsed_time = time() - start_time
    
    if elapsed_time > 1800:
        signal = "Time Over"
        break
    
    if tweetCount >= maxTweets:
        signal = "Required number of tweets obtained"
    
    print("Downloaded {0} tweets".format(tweetCount) + " On the {0} try".format(twitter_runs) + 
          " after {0}".format(elapsed_time))


# Removing Duplicates
    
Econ_Tweets = Econ_Tweets.drop_duplicates('id')
Econ_Tweets.to_csv("Sentiment_Tweets.csv", index = False)

# Computing Sentiment Score

weighted_sent = []
for i in range(Econ_Tweets.shape[0]):
    if Econ_Tweets.iloc[i,3] > 0 and Econ_Tweets.iloc[i,4] > 0:
        weighted_sent.append(Econ_Tweets.iloc[i,3]*Econ_Tweets.iloc[i,6] + (0.5*Econ_Tweets.iloc[i,4])*Econ_Tweets.iloc[i,6])
    elif Econ_Tweets.iloc[i,3] > 0 and Econ_Tweets.iloc[i,4] <= 0:
        weighted_sent.append(Econ_Tweets.iloc[i,3]*Econ_Tweets.iloc[i,6])
    elif Econ_Tweets.iloc[i,3] <= 0 and Econ_Tweets.iloc[i,4] > 0:
        weighted_sent.append((0.75*Econ_Tweets.iloc[i,4])*Econ_Tweets.iloc[i,6])
    else:
        weighted_sent.append(Econ_Tweets.iloc[i,6])
        
tweet_post_sent = [weighted_sent[i] for i in range(len(weighted_sent)) if weighted_sent[i] >= 0]    
tweet_neg_sent = [weighted_sent[i] for i in range(len(weighted_sent)) if weighted_sent[i] < 0]        
    
post_percent = round(sum(tweet_post_sent)/(sum(tweet_post_sent)-sum(tweet_neg_sent))*100,2)
neg_percent = round(-sum(tweet_neg_sent)/(sum(tweet_post_sent)-sum(tweet_neg_sent))*100,2)

last_up = current_time.isoformat()
last_up = last_up[:-7].replace('T', ' ') + " GMT"

tweets_analyzed = Econ_Tweets.shape[0]
       
# Add Log details of current scrape run

bucket = "twitsent1"
file_name = "Tweet_Log.csv"
s3 = boto3.client('s3') 
obj = s3.get_object(Bucket= bucket, Key= file_name) 
Tweet_Log = pd.read_csv(obj['Body']) # 'Body' is a key word


Tweet_Log  = pd.read_csv("Tweet_Log.csv", index_col = False)

Tweet_Log = Tweet_Log.append({'Time' : last_up, 'Start' : start_tweet_count, 'Deleted' : len(times), 
                              'Required' : maxTweets, 'Scraped' : tweetCount, 'Analyzed' : tweets_analyzed, 
                              'Duration' : elapsed_time, 'Signal' : signal, 'Pos Sent' : post_percent, 
                              'Neg Sent' : neg_percent}, ignore_index=True)
        
s3 = boto3.resource('s3')
csv_buffer = StringIO()
Tweet_Log.to_csv(csv_buffer, index = False)
s3.Object(bucket, 'Tweet_Log.csv').put(Body=csv_buffer.getvalue())









