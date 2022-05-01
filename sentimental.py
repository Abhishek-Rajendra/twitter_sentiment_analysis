from kafka import KafkaConsumer
import json
from textblob import TextBlob
from elasticsearch import Elasticsearch
from config import *

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

if __name__ == "__main__":

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers = kafka_url,
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest'
    )

    es = connect_elasticsearch()

    hashtag = filter_hashtag.replace("#", "").lower()

    # delete_index_data(es, indices)

    for message in consumer:
        data = message.value["data"]
        tweet = data["text"]
        # print(data)
        blob = TextBlob(tweet)
        polarity = blob.sentiment.polarity
        if polarity < 0:
            sentiment = "negative"
        elif polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"
        # print(tweet, sentiment)
        print("#########", sentiment)
        document = {}
        document["date"] = data["created_at"]

        if "hashtags" in data["entities"]:
            print([tag["tag"] for tag in data["entities"]["hashtags"]])
            
            for hashtags in data["entities"]["hashtags"]:
                tag = hashtags["tag"]
                if hashtags["tag"].lower() == hashtag:
                    print("is there")
                    print(tag)
                    # Add data elastic search
                    es.index(
                        index = 'tweet',
                        document = {
                                "date": data["created_at"],
                                "message": data["text"],
                                "sentiment": sentiment,
                                "hashtag": [tag["tag"] for tag in data["entities"]["hashtags"]]
                                }
                            )
                    print("------------------------------")