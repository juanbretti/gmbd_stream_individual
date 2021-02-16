# https://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
# https://spark.apache.org/docs/3.0.1/streaming-kafka-0-10-integration.html
# https://github.com/kaantas/kafka-twitter-spark-streaming/blob/master/kafka_twitter_spark_streaming.py


import kafka
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from constants import *
import json

#Create Spark Context to Connect Spark Cluster
sc = SparkContext(appName="TwitterStreamApp_JPBM")
sc.setLogLevel("ERROR")

#Set the Batch Interval is 10 sec of Streaming Context
ssc = StreamingContext(sc, 10)

#Create Kafka Stream to Consume Data Comes From Twitter Topic
#localhost:2181 = Default Zookeeper Consumer Address
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'raw-event-streaming-consumer', {KAFKA_TOPIC: 1})

#Count the number of tweets per User
user_counts = kafkaStream.map(lambda x: x[1].encode('utf-8'))

#Print the User tweet counts
user_counts.pprint()

#Start Execution of Streams
ssc.start()
ssc.awaitTermination()
