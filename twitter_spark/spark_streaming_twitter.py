from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

from constants import *

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd, header):
    print("{sep} {header} {sep} {time} {sep}".format(sep="-"*5, time=str(time), header=header))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(term=w[0], term_count=w[1]))
        # create a DF from the Row RDD
        df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        df.registerTempTable("table")
        # get the top 10 terms from the table using SQL and print them
        df_query = sql_context.sql("select term, term_count from table order by term_count desc limit 10")
        df_query.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp_JPBM")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
# Only one StreamingContext can be active in a JVM at the same time. (https://spark.apache.org/docs/2.0.0/streaming-programming-guide.html)
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
# TODO: setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp_JPBM")

# read data from the port
# TODO: read data from the port
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

# split each tweet into words
# TODO: split each tweet into words
splits = dataStream.flatMap(lambda line: line.lower().split(" "))
hashtags = splits.filter(lambda w: w.startswith("#")).map(lambda x: (x, 1))
words = splits.map(lambda x: (x.replace('#', ""), 1))

# adding the count of each hashtag to its last count using updateStateByKey
# TODO: adding the count of each hashtag to its last count using updateStateByKey
hashtags_totals = hashtags.updateStateByKey(aggregate_tags_count)
words_totals = words.updateStateByKey(aggregate_tags_count)

# do the processing for each RDD generated in each interval
hashtags_totals.foreachRDD(lambda time, rdd: process_rdd(time, rdd, "Hashtags total"))
words_totals.foreachRDD(lambda time, rdd: process_rdd(time, rdd, "Words total"))

# TODO: Instead of computing the top10 elements with Spark SQL, change the code to obtain  the  Top10  words  (not only  hashtags)  using  a  moving  window  of  10 minutes every 30 seconds. Copy & paste the result.
# Reference: http://davidiscoding.com/real-time-twitter-analysis-3-tweet-analysis-on-spark
words_window = words.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 10*60) # 10 minutes
words_window.foreachRDD(lambda time, rdd: process_rdd(time, rdd, "Words window"))

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
