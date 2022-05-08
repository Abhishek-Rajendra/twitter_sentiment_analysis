'''
References: 
'''
import json
import shutil
from textblob import TextBlob
from elasticsearch import Elasticsearch
from config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os

# Import 3rd party packages
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.elasticsearch:elasticsearch-spark-20_2.11:8.1.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 pyspark-shell'

def delete_index_data(es, indices):
    es.delete_by_query(index=indices, body={"query": {"match_all": {}}})


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([elastic_url], ssl_show_warn=False, verify_certs=False, basic_auth=(user, password))
    print(_es.info())
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

def preprocessing(lines):
    words = lines.select(lines.key.alias("Hashtag"), lines.value.alias("word"), lines.timestamp.alias("timestamp"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
def polarity_detection(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity < 0:
        sentiment = "negative"
    elif polarity == 0:
        sentiment = "neutral"
    else:
        sentiment = "positive"
    return sentiment

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

def hashtag(key):
    return key

def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words


if __name__ == "__main__":

    if not use_checkpoint:
        try: 
            if os.path.isdir(checkpoint_sentiment):
                shutil.rmtree(checkpoint_sentiment)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (checkpoint_sentiment, e))

    spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Sentimental.analysis")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.elasticsearch:elasticsearch-spark-20_2.11:8.1.3')\
    .getOrCreate()
    
    lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_url) \
    .option("subscribe", topic_name) \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)") \

    
    # with open("temp/temp.json") as fp:
    #     schema = StructType.fromJson(json.load(fp))

    # new_schema = StructType.fromJson(json.loads(schema))
    # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    
    # streamData = df.selectExpr("CAST(value AS STRING)")

    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    words = words.repartition(1)
    # query = words.writeStream.queryName("all_tweets")\
    #     .outputMode("append").format("parquet")\
    #     .option("path", "./parc")\
    #     .option("checkpointLocation", "./check")\
    #     .trigger(processingTime='60 seconds').start()
    # query.awaitTermination()
    
    query = words.writeStream \
    .outputMode("append") \
    .queryName("writing_to_es") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes.wan.only", "true") \
    .option("es.net.ssl", "false") \
    .option("checkpointLocation", checkpoint_sentiment) \
    .option("es.resource", index) \
    .option("es.nodes", host) \
    .option("es.port", port)\
    .start()

    query.awaitTermination()

    # words.writeStream \
    # .outputMode("append") \
    # .format("console") \
    # .start().awaitTermination()

    # es = connect_elasticsearch()


    # delete_index_data(es, indices)

    # for message in consumer:
    #     data = message.value["data"]
    #     tweet = data["text"]
    #     # print(data)
    #     blob = TextBlob(tweet)
    #     polarity = blob.sentiment.polarity
    #     if polarity < 0:
    #         sentiment = "negative"
    #     elif polarity == 0:
    #         sentiment = "neutral"
    #     else:
    #         sentiment = "positive"
    #     # print(tweet, sentiment)
    #     print("#########", sentiment)
    #     document = {}
    #     document["date"] = data["created_at"]

    #     if "hashtags" in data["entities"]:
    #         print([tag["tag"] for tag in data["entities"]["hashtags"]])
            
    #         for hashtags in data["entities"]["hashtags"]:
    #             tag = hashtags["tag"]
    #             if hashtags["tag"].lower() == hashtag:
    #                 print("is there")
    #                 print(tag)
    #                 # Add data elastic search
    #                 es.index(
    #                     index = 'tweet',
    #                     document = {
    #                             "date": data["created_at"],
    #                             "message": data["text"],
    #                             "sentiment": sentiment,
    #                             "hashtag": [tag["tag"] for tag in data["entities"]["hashtags"]]
    #                             }
    #                         )
    #                 print("------------------------------")