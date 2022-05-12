# Kafka properties
kafka_url = "localhost:9092"
topic_name = "basic"
topic_names = [topic_name]

# Elasticsearch properties
elastic_url = "https://localhost:9200"
host = "localhost"
port = 9200
user = "elastic"
# password = "elastic_search_password"
index = "tweet_sentimental_analysis"
indices = [index]

# Checkpoints
use_checkpoint = False
checkpoint_sentiment = "checkpoint/sentiment"
checkpoint_clustering = "checkpoint/clustering"

# Tweeter API

# Include only one hashtag for clustering code

hashtags = ["doctorstrangeinthemultiverseofmadness", "RRR", "kgf"]

"""API ACCESS KEYS"""
bearer_token = "generate_from_twitter_api_developers"

