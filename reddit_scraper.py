#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 18 16:31:19 2023

@author: jessicawitte
"""

#a python program to scrape data from reddit using the pushshift api 
#this is the wooden roller coaster edition; suggestions welcome (jessica.witte@ed.ac.uk)

#1. import packages
import pandas as pd
import json
import requests
import datetime
import time

#2. choose your URL:
    
#for scraping submissions
url = "https://api.pushshift.io/reddit/search/submission/"

#for scraping comments
url = "https://api.pushshift.io/reddit/search/comment/"

#3. define parameters:
    #n.b. all of these are optional; additional params defined on the pushshift repo readme
    
#params for comments:
subreddit='Scotland'
keyword='macaroni pie'
#date format is Year, Month, Date, Hour, Minute
start_date=int(datetime.datetime(2020, 12, 10, 0, 0).timestamp()) 
size= #integer<=500
end_date=int(datetime.datetime(2022, 12, 13, 0, 0).timestamp())
ids=#for finding specific comments by ID

#params for submissions:
'title'='keyword' #searches the post title field *only* for matching string(s)
'selftext'='keyword' #searches submission body *only* 
#title:not, q:not, selftext:not to exclude terms
'score'=#find posts with a certain score or range e.g. x > 20
'num_comments'=#limit to posts with a specified number/range of comments only


params={'subreddit': subreddit, 'q': keyword, 'size': size, 'after': start_date,
        'before': end_date, 'sort': 'desc' #or 'asc', 
        'sort_type': 'created_utc' #or 'num_comments', 'score'}
        
#4. create storage variables (AKA pythonic filing cabinets):
        #DISCLAIMER: no reptiles were confined or otherwise harmed in the creation of this dataset

comments=[]
comment_ids=set()
#optional, for the organised: rename the variables according to your search, e.g. "submissions" and "submission_ids" 
        
#set counter/limit for the "except" loop (in case of failure):
try_count=0
try_limit=3

#5. scrape the data!
while try_count < try_limit:
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        #loop through the subreddit until there's nothing else to scrape 
        #within the params:
        if len(data['data']) == 0:
            break
        
        #OR to scrape only a certain # of posts:
        if len(comment_ids) > 250:
            break
            #n.b. define sort_type, asc/desc and other params for this option
        
        for comment in data['data']:
            if comment['id'] not in comment_ids:
                comments.append(comment)
                comment_ids.add(comment['id'])
            
        comments.extend(data['data'])
        params['before'] = data['data'][-1]['created_utc']
        time.sleep(1)
    
    #in case of error
    except requests.exceptions.RequestException as i:
        print ('Suspected API error encountered:', i)
        try_count += 1
        time.sleep(1)
        
        #and retry up to 3 times. this is optional, but useful for larger scraping tasks
        if try_count >= try_limit:
            print('The third time was not a charm. Scrape failed; exiting.')
            break
    
    
#6. dump data into pandas dataframe and perform some very perfunctory cleaning (hide the floordrobe in the wardrobe):
df=pd.read_json(json.dumps(comments))

#remove duplicate data (pushshift creates lots of duplicates--insights here welcome!)
df=df.drop_duplicates(subset='id')
df.info()

#subset by columns to keep of the ~80-100 or so from pushshift:
    #n.b. print the column titles to check them first. they change with every scrape!
df=df[['subreddit', 'selftext', 'title', 'upvote_ratio', 
       'awards_received', 'link_flair_text', 'removed_by_category', 'id', 
       'author', 'url', 'permalink', 'created_utc', 'num_comments']]

#change unix time to human time
df['created_utc']=pd.to_datetime(df['created_utc'], unit='s')

#7. save output
df.to_csv('/filepath/reddit_data.csv')
