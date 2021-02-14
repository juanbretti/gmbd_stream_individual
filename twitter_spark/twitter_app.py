import socket
import sys
import requests
import requests_oauthlib
import json

from constants import *

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # query_data = [('locations', '-9.7426931269,35.9204747004,3.6606271856,43.99330876'), ('track', '#')] # Spain
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track','#')] # Somewhere in US
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines(): 
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            tweet_screen_mame = full_tweet['user']['screen_name']
            tweet_place = full_tweet['place']['full_name']
            tweet_country = full_tweet['place']['country']
            tweet_lang = full_tweet['lang']
            print("Tweet Text: " + tweet_text)
            print("Message written by {} in {}, {}, in the language {}.".format(tweet_screen_mame, tweet_place, tweet_country, tweet_lang))
            print("-"*20)
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

conn = None

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

conn, addr = s.accept()
print("Connected... Starting getting tweets.")

resp = get_tweets()
send_tweets_to_spark(resp, conn)

# Bounding box
# https://boundingbox.klokantech.com/
# Buenos Aires
# [[[-63.39279324,-41.03727931],[-56.66467158,-41.03727931],[-56.66467158,-33.26161195],[-63.39279324,-33.26161195],[-63.39279324,-41.03727931]]]
# -63.39279324,-41.03727931,-63.39279324,-41.03727931
# Madrid
# [[[-3.88895392,40.31197738],[-3.51791632,40.31197738],[-3.51791632,40.64372926],[-3.88895392,40.64372926],[-3.88895392,40.31197738]]]
# -3.7834,40.3735,-3.6233,40.4702
# Spain
# [[[-9.7426931269,35.9204747004],[3.6606271856,35.9204747004],[3.6606271856,43.99330876],[-9.7426931269,43.99330876],[-9.7426931269,35.9204747004]]]
# -9.7426931269,35.9204747004,3.6606271856,43.99330876
# Original
# query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]

# Problemas
# Cuando busco en 'es', trae muchos menos
# Cuando busco en Buenos Aires, no trae nada
# Cuando cancelo con Ctrl + C, tengo que esperar para que la conexion se termine/expire
