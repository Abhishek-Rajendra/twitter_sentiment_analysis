
from tweepy import StreamRule
from tweepy import StreamingClient
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from config import *


# To perform admin operation like create and delete topics
admin_client = KafkaAdminClient(bootstrap_servers=[ip_address])

# Create new topics from list
def create_topics(topic_names):
    consumer = KafkaConsumer(
    bootstrap_servers = ip_address,
    )
    existing_topic_list = consumer.topics()
    print(list(consumer.topics()))
    topic_list = []
    for topic in topic_names:
        if topic not in existing_topic_list:
            print('Topic : {} added '.format(topic))
            topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        else:
            print('Topic : {topic} already exist ')
    try:
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic Created Successfully")
        else:
            print("Topic Exist")
    except TopicAlreadyExistsError as e:
        print("Topic Already Exist")
    except  Exception as e:
        print(e)

# Delete topics from the list provided
def delete_topics(topic_names):
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError as e:
        print("Topic Doesn't Exist")
    except  Exception as e:
        print(e)


# Twitter Stream Listener
class KafkaPushListener(StreamingClient):
    def __init__(self, bearer_token):
        StreamingClient.__init__(self, bearer_token)
        self.producer = KafkaProducer(bootstrap_servers=[kafka_url])

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter Streaming API
        self.producer.send(topic_name, data)
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":

    delete_topics(topic_names)
    streaming_client = KafkaPushListener(bearer_token)
    create_topics(topic_names)

    streaming_client.add_rules(StreamRule(value=filter_hashtag))

    # Print exiting rules applied on to the Twitter API - For filtering thr streaming data
    print(streaming_client.get_rules())

    # Uncomment and update id vales to delete rules based in ids
    # streaming_client.delete_rules(ids=["1518016285157867520","1518857090021928960"])

    streaming_client.filter( tweet_fields=["text","created_at","entities"])