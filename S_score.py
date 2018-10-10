
# coding: utf-8

# In[2]:


import tweepy
import csv
import codecs
from datetime import datetime, timedelta
import time
import numpy as np
import pandas as pd
from urllib.parse import urlparse
import mysql.connector
from flask import Flask, jsonify, abort, make_response
import peewee as pe


# In[3]:


url = urlparse('mysql://user:pass@localhost:3306/dbname')

conn = mysql.connector.connect(
    host = 'localhost',
    port = 3306,
    user = 'root',
    password = 'whiteoak',
    database = "buzzri_database"
)

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS `tdnet_table`")
cursor.execute("""CREATE TABLE IF NOT EXISTS `tdnet_table` (
`tdnet_id` int(10) NOT NULL,
`stock_id` int(10) NOT NULL,
`date` timestamp NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""")
cursor.execute("INSERT INTO tdnet_table VALUES (%s,%s,%s)",(0,7203,"2018-10-05 14:00:00"))
cursor.execute("INSERT INTO tdnet_table VALUES (%s,%s,%s)",(1,8001,"2018-10-06 14:00:00"))
conn.commit()
conn.close()


# In[4]:


url = urlparse('mysql://user:pass@localhost:3306/dbname')

conn = mysql.connector.connect(
    host = 'localhost',
    port = 3306,
    user = 'root',
    password = 'whiteoak',
    database = "buzzri_database"
)

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS `stock_table`")
cursor.execute("""CREATE TABLE IF NOT EXISTS `stock_table` (
`stock_id` int(10) NOT NULL,
`name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""")
cursor.execute("INSERT INTO stock_table VALUES (%s,%s)",(7203,"トヨタ"))
cursor.execute("INSERT INTO stock_table VALUES (%s,%s)",(8001,"伊藤忠"))
conn.commit()
conn.close()


# In[5]:


fina_api = [

  {

    "id": "@uwet654",

    "key": "ODcxDMwBccnXzT9JkGL2UPkMc",

    "secret": "cCEgOEnYUF3W4uwHc5hjxsc8O2oQbxQ62sm37IEM5CGhOPUj7T",

    "token": "800635150962860033-Xs8okPrQz52G7ha9eoLVgZ5viJAqeKu",

    "token_secret": "67dhvvpdoLJjw9uBJbhgyp5PhFrG10sdGQpmX57N96FPu",

    "idNum": 0

  },

  {

    "id": "@hertn8233",

    "key": "VUN82cqFxWlrbqYsTE5Y7zPWD",

    "secret": "d1tXyigrklkaVbrgZfjlEfzICfq30gOpKTo247gpt6Okfk1Obv",

    "token": "800635938011389952-CWNvpgqaZOfH6vfZBz3HXNFvnuDSXAs",

    "token_secret": "8s0hfBj6p4YgabpX9HCDpoRD3LM3Q9sSodMPZBxnLQ51M",

    "idNum": 1

  },

  {

    "id": "@aowier_124",

    "key": "m4g5cDpl6YtglIF2v2PaWUBCP",

    "secret": "bB3ZsJtBn9CZOnwngIfwJbgKTWHM4sJVsoMo2aAB80LXkZCOvM",

    "token": "800636417005105152-JtYCVIDhzoWsbWFUJkQAFTPq4tW8MCf",

    "token_secret": "KevFT47AQyZjtwHXaIamjD8HcRefnwVGJHyt5HafZbDRQ",

    "idNum": 2

  },

  {

    "id": "@pojaaaaaaaa",

    "key": "z1DXUJXKKXvEJ4ICReWhm3ZlY",

    "secret": "mzYoRFPYTnjrE9f8zBAiO5aDBQtPb1iWXZSMFdkY2dfAACZEr8",

    "token": "800636866387001344-vTZ9LS3ZIDlksV7ivRGFXWFvOs5LJPT",

    "token_secret": "8Kkdhs1Pqil96j31aAbwSDpEqPwRzvdJAJeKJwp5Fb0Hn",

    "idNum": 3

  },

  {

    "id": "@kearji43",

    "key": "6AmnrqnSHuNIp9MSYzWGBdKVm",

    "secret": "HuNwtef88e1SBPfeRIHwRUzn0NkLjT7RtdQpTjqH1Tkyt4SiMi",

    "token": "800688079770001409-AoKcNELcrFmwi4ppZJKoZrSmh2DqeIJ",

    "token_secret": "0uXOtlsBwAjXlbkW7H7XoGvsyhvDcNGTnX98nWRTcOwyA",

    "idNum": 4

  },

  {

    "id": "@tako903kkk",

    "key": "RmKbG4ZGjbuXKiqW74iMwbcQk",

    "secret": "A85LRRk18Ms0K70CJMcNooT5Wu5fvW4GyhMp5CB5wsDoacdlSI",

    "token": "800690842809737216-FZm9Dro3M0xjgXd8W5WyPKLvGAnuBwh",

    "token_secret": "g5GKheV5R6feBevEHRqycPCh4eM4YQAyAqfW8XbeuujVE",

    "idNum": 5

  },

  {

    "id": "@ikayaki_33",

    "key": "yILsu06Jc7Flr2SryKsfsRqJQ",

    "secret": "sKkbtxc6bPzxqwPy8aTd50M46bIGgDYqrdH06oFjPNcqAGuI15",

    "token": "801308872766001152-MStJVGG5NgQI0aIYQnwnwAkjJs3vkSC",

    "token_secret": "A0CKCPYcRsb20Q8XKlDvyx2YzhopBlX2g8t7qtUhld9lo",

    "idNum": 6

  },

  {

    "id": "@nasu_yaki",

    "key": "S9eKT1hHmKKnRV2cuhudrzJFo",

    "secret": "MPqVWZNfcKR3HfKE1saay8kgvF9iaUhgmPtsHF2f803zx0BOMC",

    "token": "801311173442498560-fjgYJfxPS4fZ10A1ijx5B0v21ia2GAA",

    "token_secret": "RwQKaNzbV70v7wPQA1F52AzdswhldvWFGzt7MYIXPYlcO",

    "idNum": 7

  },

  {

    "id": "@oka82_d",

    "key": "JpOKSydsQ9zH6vOtNcWNPBEQ9",

    "secret": "xTaZkUd4b7xykfOL00r5TGj8SzlGChkad834pKFgftd4ZaeYIY",

    "token": "801313985521979392-l63P3qiXjY8EfS8CSsJxO3zC6RJ5XTm",

    "token_secret": "XUwbF8zg9Mjwh7AroAvEcYZ8urSBZptkAta1P8E4l3FNT",

    "idNum": 8

  },

  {

    "id": "@na_68KM",

    "key": "ClovhST0n3uD1kAFvCu3WbNj0",

    "secret": "jvFRFNZrF5G2czn4ulfYS0ZbSV5qe7C1KvCB7sGOzKWaaqXc4E",

    "token": "801315676258496513-NQk4Ca3IBkiOiNAJCvQruhUMor0zuUq",

    "token_secret": "j0yZXBZ6V9dbuZKoAcYcXsUUtIFG0jQD5fnFm6jY7m1RS",

    "idNum": 9

  },

  {

    "id": "@tyt_yoh399",

    "key": "Q9XKmm8dQYsT3khyOCvVC9HUe",

    "secret": "vrxkJqngULJJuzMd0T31cYnaZ3h841To9M8uLngWgSbKbeApTE",

    "token": "808888906967986176-HrDpeGWhWpJf6VE7lmxN5MHpnNyHLjF",

    "token_secret": "m724vvEs6V4xbX9rt9v7zJP76Ky6m58kh78B2QcoPUI5q",

    "idNum": 10

  },

  {

    "id": "@OO2TC4",

    "key": "z8IEPiLKGErx3EQckNSMD9FbZ",

    "secret": "XMywcZJvfhB65qqATlE11gU4CBEojaBPFUPJbMQqU3QSkQzWuS",

    "token": "809002581766782976-uykC10hlzP5MlQU30EW6umdiKDv6fK3",

    "token_secret": "tAXBtmTeuYG9o1LsuJgPnHSk9ay0h1BZV8kM72bST3cKX",

    "idNum": 11

  },

  {

    "id": "@jy_hiro_t9",

    "key": "0cJeCwEmYQhqtpKyrXV9YZX5z",

    "secret": "3NThxdaMnSubwCiO39qvM7ttmDkZnAp1KyZsegABAFA65YF7sk",

    "token": "809009899082371072-UI2GQTfmPPeFOsMr5wFZDJ318lHOxts",

    "token_secret": "3PZXe74wzd22mkmSi0ITZ6eheU8PNBtwfAlrAse3r5UHl",

    "idNum": 12

  },

  {

    "id": "@m_m61pa",

    "key": "IP4pT9zJ0tnvPtmZyPQWsLgmb",

    "secret": "NK3E9XwovRAIRtk8pnfTBONF89K4qdVCdhS80Y2t4xVzI1Zihp",

    "token": "809019063020072962-r4PgIEspwGQtB6MESHpd9D1fjc2o6gd",

    "token_secret": "9PQl00BVyjuFKvRhkaLn2CLpUlyifrkx8Lxd3fkwXyJ92",

    "idNum": 13

  },

  {

    "id": "@A4sDGzqCugRQTVo",

    "key": "RMYsaIdGvBtcMIRJLEvVQqpOK",

    "secret": "QGM7V1IY8UFRgRo16Gtp1RQhjUYrYaELqjf2Uq6vXK06rjRvOw",

    "token": "809019579158564864-BcF3RFyjGqFTxQsCqCbnpu3gt9dFbmY",

    "token_secret": "viWtBtr0ys3C7QoeF0iPXYQycK9u6UjrkfTLK5IzfkMZa",

    "idNum": 14

  },

  {

    "id": "@TtyptoOni",

    "key": "X5uBSY882IpcNthaSMekxVbOU",

    "secret": "DSH4MTlSjxyAwLHUG3ZGnBgITdKKEjoGKU4DkJyR0PAtGXvPYK",

    "token": "812132314817773568-IkHpH4bLLqC1rVnZsYLKtxrJG9iXBzR",

    "token_secret": "9IpEzXlJ8RymcDEHjxp1Z4gFaul9Fk44C3iAZtjP5zgn0",

    "idNum": 15

  },

  {

    "id": "@ei45972",

    "key": "VILtvhyltI76vmXKvb7bpAR4M",

    "secret": "lIZWt3lg0b9xxPq3sT0XEdT3ItEo5aXhKEp44K4nbvC1MAC9rw",

    "token": "812133285362946048-w4eqKBJt5A60lc52YLv3KLPgDBS2Eez",

    "token_secret": "utteE1314L6k4lAZx4PgnMq94Zyp2CRzT1lzBzuPEC8uG",

    "idNum": 16

  }

]


# In[6]:


api_list = []

for i in range(len(fina_api)):
    CK = fina_api[i]["key"]
    CS = fina_api[i]["secret"]
    AT = fina_api[i]["token"]
    ATS = fina_api[i]["token_secret"]
    auth = tweepy.OAuthHandler(CK, CS)
    auth.set_access_token(AT, ATS)
    api= tweepy.API(auth)
    api_list.append(api)
    
api_count = 0


# In[7]:


def tweet_search(since_time, until_time, keyword, api_list):
    global api_count
    api = api_list[api_count]
    until_time = until_time.strftime('%Y-%m-%d_%H:%M:%S_JST')

    tweets = []
    try:
        result = api.search(q=keyword+" -rt", until=until_time, lang="ja", count=100)
        for i in range(len(result)):
            time_ = result[i].created_at + timedelta(hours=9)
            tweets.append(time_.strftime('%Y-%m-%d_%H:%M:%S_JST'))
    except:
        api_count += 1
        api = api_list[api_count]
        result = api.search(q=keyword+" -rt", until=until_time, lang="ja", count=100)
        for i in range(len(result)):
            time_ = result[i].created_at + timedelta(hours=9)
            tweets.append(time_.strftime('%Y-%m-%d_%H:%M:%S_JST'))
    
    if tweets != []:
        while datetime.strptime(tweets[-1], '%Y-%m-%d_%H:%M:%S_JST') > since_time:
            try:
                last_time = tweets[-1]
                result = api.search(q=keyword+" -rt", until=last_time, lang="ja", count=100)
                if result == []:
                    break
                else:
                    for i in range(len(result)):
                        time_ = result[i].created_at + timedelta(hours=9)
                        tweets.append(time_.strftime('%Y-%m-%d_%H:%M:%S_JST'))
            except:
                api_count += 1
                api = api_list[api_count]
    
    if tweets != []:
        while datetime.strptime(tweets[-1], '%Y-%m-%d_%H:%M:%S_JST') < since_time:
            del tweets[-1]
            if tweets == []:
                break

    return tweets


# In[8]:


def calc_buzzri(date,name):
    announce_time = date
    since_time = date
    until_time = date + timedelta(minutes=5)
    
    std_since_time = since_time - timedelta(minutes=60*5)
    std_until_time = date

    tweets = tweet_search(since_time, until_time, name, api_list)
    std_tweets = tweet_search(std_since_time, std_until_time, name, api_list)

    y = len(tweets)
    std_y = len(std_tweets)

    if np.sum(std_y) == 0:
        buzzri = np.nan
    else:
        buzzri = np.sum(y) / (np.sum(std_y)/(60*5)*5)

    return buzzri


# In[9]:


def write_db():
    db = pe.MySQLDatabase('buzzri_database', user='root', password='whiteoak',port=3306, host='localhost')
    db.connect()
    query_0 = 'SELECT * FROM tdnet_table;'
    with db.cursor(pe.DictCursorWrapper) as cursor:
        cursor.execute(query_0)
        td_stock_date = cursor.fetchall()

    query_1 = 'SELECT * FROM stock_table;'
    with db.cursor(pe.DictCursorWrapper) as cursor:
        cursor.execute(query_1)
        stock_name = cursor.fetchall()
    db.close()
    
    for i in range(len(td_stock_date)):
        buzzri = calc_buzzri(td_stock_date[i][2],stock_name[i][1])
        url = urlparse('mysql://user:pass@localhost:3306/dbname')
        conn = mysql.connector.connect(
            host = 'localhost',
            port = 3306,
            user = 'root',
            password = 'whiteoak',
            database = "buzzri_database"
        )

        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS `buzzri_table` (
        `tdnet_id` int(10) NOT NULL,
        `stock_id` int(10) NOT NULL,
        `buzzri` float(6,2) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""")
        cursor.execute("INSERT INTO buzzri_table VALUES (%s,%s,%s)",(td_stock_date[i][0], td_stock_date[i][1], float(buzzri)))
        conn.commit()
        conn.close()


# In[10]:


write_db()

db = pe.MySQLDatabase('buzzri_database', user='root', password='whiteoak',port=3306, host='localhost')

class UnknownField(object):
    def __init__(self, *_, **__): pass

api = Flask(__name__)

# itemの詳細情報を取得
@api.route('/items/<int:id>', methods=['GET'])
def getItemData(id):
    global data
    db.connect()
    try:
        query = 'SELECT * FROM buzzri_table;'
        with db.cursor(pe.DictCursorWrapper) as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
        

    except:
        db.close()
        abort(404)

    db.close()
    for i in range(len(data)):
        if data[i][0] == id:
            buzzri = data[i][2]
    return make_response(jsonify(buzzri))
    
@api.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    api.run(port=80,debug=False)

