import sys
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer

from constants import *

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('locations', '-9.7426931269,35.9204747004,3.6606271856,43.99330876'), ('track', 'Filomena')] # Spain, https://boundingbox.klokantech.com/
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def repair_encoding(x):
    return x.encode('ascii', 'ignore').decode('ascii').replace('\n', ' ').replace('\t', ' ')

def send_tweets_to_kafka(http_resp, producer, topic):
    for line in http_resp.iter_lines(): 
        try:
            # JSON load
            full_tweet = json.loads(line)
            # Extraction
            tweet_text = repair_encoding(full_tweet['text'])
            tweet_screen_mame = repair_encoding(full_tweet['user']['screen_name'])
            tweet_place = repair_encoding(full_tweet['place']['full_name'])
            tweet_country = repair_encoding(full_tweet['place']['country'])
            tweet_lang = repair_encoding(full_tweet['lang'])
            # Print
            print("Tweet Text: " + tweet_text)
            print("Message written by {} in {}, {}, in the language {}.".format(tweet_screen_mame, tweet_place, tweet_country, tweet_lang))
            print("-"*20)
            # Send to Kafka
            producer.send(topic, value=tweet_text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

http_resp = get_tweets()
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                         value_serializer=lambda x: x.encode('utf-8'))
send_tweets_to_kafka(http_resp, producer, KAFKA_TOPIC)
