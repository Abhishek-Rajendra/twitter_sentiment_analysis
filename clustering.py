from matplotlib import pyplot as plt
from sentence_transformers import SentenceTransformer
from sklearn.cluster import MiniBatchKMeans
import pandas as pd
from sentimental import connect_elasticsearch
from pyspark.sql import SparkSession
from config import *
import time
import os

# Import 3rd party packages
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 pyspark-shell'


# The model maps sentences & paragraphs to a 384 dimensional dense
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')


#Our sentences we like to encode
sentences = ['This framework generates embeddings for each input sentence fsd s ddskf kjhj h  fjh fjld shgj hjkfghsjkd ghjksfhg kjsfhgjk shdgjk hfjlg hls ghjksf hgljk sfhdgj hslfj ghjfsd hgljf dhgljshfgjlkfhgjkl hsdfghf jlhkjfh gkjf shdgjk fhjl ghfslgj pre ogjklsfngsuhgirojgrgoksfk ',
    'Sentences are passed as a list of string.',
    'The quick brown fox jumps over the lazy dog.']

#Sentences are encoded by calling model.encode()
embeddings = model.encode(sentences)

# print(embeddings.shape)

# kmeans = KMeans(n_clusters=18, random_state=0).fit(embeddings)
# Y=kmeans.labels_
# z = pd.DataFrame(Y.tolist())
labels = ["0" , "1", "2"]
kmeans = MiniBatchKMeans(n_clusters=len(labels),
                                random_state=0)


# def Kmeans(time, rdd):
#   print("===========-----> %s <-----===========" % str(time))

#   embeddings = model.encode(data)

#   final = kmeans.partial_fit(embeddings)

#   #plotting the results:
#   for i in labels:
#       plt.scatter(data[final == i , 0] , data[final == i , 1] , label = i)
#   plt.legend()
#   plt.show()


#   try:
#       spark = getSparkSessionInstance(rdd.context.getConf())

#       rowRdd = rdd.map(lambda w: Row(branch=w['branch'],
#                                       currency=w['currency'],
#                                       amount=w['amount']))
                                      
#       testDataFrame = spark.createDataFrame(rowRdd)

#       testDataFrame.createOrReplaceTempView("treasury_stream")

      

  # except Exception as e:
  #     print("--> Opps! Is seems an Error!!!", e)

def get_values(values):
  return values.tolist()

if __name__ == "__main__":

    spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Sentimental.analysis")\
    .getOrCreate()

    while(True):
      lines = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_url) \
      .option("subscribe", topic_name) \
      .option("kafkaConsumer.pollTimeoutMs", 1000) \
      .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \

      # data = lines.select(lines.value).collect().writeStream \
      # .format("console") \
      # .start() 

      print(len(lines.collect()))
      data = lines['value'].tolist()

      embeddings = model.encode(data)

      final = kmeans.partial_fit(embeddings)

      #plotting the results:
      for i in labels:
          plt.scatter(data[final == i , 0] , data[final == i , 1] , label = i)
      plt.legend()
      plt.show()

      time.sleep(30)


    
    # data.awaitTermination()

    


