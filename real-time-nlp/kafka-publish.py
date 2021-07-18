import time

import tweepy
from kafka import KafkaProducer
from json import dumps
import config as conf

auth = tweepy.OAuthHandler(conf.api_key, conf.api_secret)
auth.set_access_token(conf.access_token, conf.access_token_secret)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                         )
api = tweepy.API(auth)


def search_tweet():
    search_result = api.search(q='Corona', lang='en', rpp=10)

    for tweet in search_result:
        data = tweet._json
        tw = {
            'id': data['id'],
            'tweet': data['text'],
            'Creation_date': data['created_at'],
            'UserName': data['user']['name']
        }
        producer.send('twitter', tw)
    producer.flush()


if __name__ == '__main__':
    while True:
        print("Publish new tweets: ")
        search_tweet()
        time.sleep(30)
