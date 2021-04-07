from psaw import PushshiftAPI
import datetime as dt
from datetime import datetime
import csv
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')

symbols_data1=pd.read_csv("nasdaqlisted.csv")
symbols_data2 = pd.read_csv("otherlisted.csv")
frames = [symbols_data1, symbols_data2]
symbols_data = pd.concat(frames)
symbols=list(symbols_data['Symbol'].values)

stop = stopwords.words('english')
stop = [each_string.upper() for each_string in stop]
symbols = [i for i in symbols if i not in stop]

def listToString(s): 
    str1 = ""   
    for ele in s: 
        str1 += ele  
    return str1

api = PushshiftAPI()

start_time = int(dt.datetime(2008, 6, 27).timestamp())

submissions = (api.search_submissions(after = start_time,
                                          subreddit = 'stocks',
                                          filter = ['url', 'author', 'title', 'subreddit', 'selftext']
                                          #,limit = 100
                                          ))
stocks_df = pd.DataFrame(columns=['Date', 'Title', 'Body'])
for submission in submissions:
  try:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), submission.selftext)
    stocks_df = stocks_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), 'Title': submission.title, 
                                    'Body':submission.selftext}, ignore_index=True)
  except:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'))
    stocks_df = stocks_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), 'Title': submission.title},
                     ignore_index=True)

titles_stocks=stocks_df['Title'].values
date_stocks = stocks_df['Date'].values
body_stocks = stocks_df['Body'].values

# # Checks for Symbols in Titles
s=[]
for i in titles_stocks:
    k=[]
    m1=re.sub('[^A-Za-z0-9]+', ' ', i)
    i=[m.lower() for m in m1.split()]
    for j in symbols:
        if j.lower() in i:
            k.append(j)
    s.append(k)

a={'Date': date_stocks, 'Symbols':s,'Title':titles_stocks, 'Body':body_stocks}
df2_stocks = pd.DataFrame(a)
df2_stocks = df2_stocks[~df2_stocks.Symbols.str.len().eq(0)]
df2_stocks = df2_stocks.reset_index()
#df2_stocks

x = df2_stocks['Symbols']

for i in range(len(df2_stocks)):
  if len(x[i])>1:
    list_1 = x[i]
    for j in range(len(list_1)):
      output_csv_file = "stocks"+"_"+listToString(list_1[j])+"_r.csv"
      df2_stocks.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')
  else:
    output_csv_file = "stocks"+"_"+listToString(x[i])+"_r.csv"
    df2_stocks.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')

api = PushshiftAPI()

start_time = int(dt.datetime(2012, 1, 31).timestamp())

submissions = (api.search_submissions(after = start_time,
                                          subreddit = 'wallstreetbets',
                                          filter = ['url', 'author', 'title', 'subreddit', 'selftext']
                                          #,limit = 100
                                          ))

wsb_df = pd.DataFrame(columns=['Date', 'Title', 'Body'])
for submission in submissions:
  try:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), submission.selftext)
    wsb_df = wsb_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), 'Title': submission.title,
                            'Body': submission.selftext}, ignore_index=True)

  except:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'))
    wsb_df = wsb_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), 'Title': submission.title},
                    ignore_index=True)

titles_wsb=wsb_df['Title'].values
date_wsb = wsb_df['Date'].values
body_wsb = wsb_df['Body'].values

s=[]
for i in titles_wsb:
    k=[]
    m1=re.sub('[^A-Za-z0-9]+', ' ', i)
    i=[m.lower() for m in m1.split()]
    for j in symbols:
        if j.lower() in i:
            k.append(j)
    s.append(k)

b={'Date': date_wsb, 'Symbols':s, 'Title':titles_wsb, 'Body':body_wsb}
df2_wsb = pd.DataFrame(b)
df2_wsb = df2_wsb[~df2_wsb.Symbols.str.len().eq(0)]
df2_wsb = df2_wsb.reset_index()
#df2_wsb

y = df2_wsb['Symbols']

for i in range(len(df2_wsb)):
  if len(y[i])>1:
    list_2 = y[i]
    for j in range(len(list_2)):
      output_csv_file = "wsb"+"_"+listToString(list_2[j])+"_r.csv"
      df2_wsb.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')
  else:
    output_csv_file = "wsb"+"_"+listToString(y[i])+"_r.csv"
    df2_wsb.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')

api = PushshiftAPI()

start_time = int(dt.datetime(2007, 6, 27).timestamp())

submissions = (api.search_submissions(after = start_time,
                                          subreddit = 'investing',
                                          filter = ['url', 'author', 'title', 'subreddit', 'selftext']
                                          #,limit = 40
                                          ))

investing_df = pd.DataFrame(columns=['Date', 'Title', 'Body'])
for submission in submissions:
  try:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'), submission.selftext)
    investing_df = investing_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                         'Title': submission.title, 'Body': submission.selftext},ignore_index=True)

  except:
    print(datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'))
    investing_df = investing_df.append({'Date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                         'Title': submission.title},ignore_index=True)

titles_investing=investing_df['Title'].values
date_investing = investing_df['Date'].values
body_investing = investing_df['Body'].values

s=[]
for i in titles_investing:
    k=[]
    m1=re.sub('[^A-Za-z0-9]+', ' ', i)
    i=[m.lower() for m in m1.split()]
    for j in symbols:
        if j.lower() in i:
            k.append(j)
    s.append(k)
c={'Date': date_investing, 'Symbols':s, 'Title':titles_investing, 'Body': body_investing}
df2_investing = pd.DataFrame(c)
df2_investing = df2_investing[~df2_investing.Symbols.str.len().eq(0)]
df2_investing = df2_investing.reset_index()
#df2_investing

z = df2_investing['Symbols']

for i in range(len(df2_investing)):
  if len(z[i])>1:
    list_3 = z[i]
    for j in range(len(list_3)):
      output_csv_file = "investing"+"_"+listToString(list_3[j])+"_r.csv"
      df2_investing.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')
  else:
    output_csv_file = "investing"+"_"+listToString(z[i])+"_r.csv"
    df2_investing.loc[[i],['Date', 'Symbols', 'Title', 'Body']].to_csv(output_csv_file,
           index=False,
           header=False,
           mode='a')