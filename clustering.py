from matplotlib import pyplot as plt
from sentence_transformers import SentenceTransformer
from pyspark.sql import SparkSession
from config import *
import os
import time
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors, VectorUDT
import seaborn
seaborn.set(style='ticks')




# Import 3rd party packages
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 pyspark-shell'


# The model maps sentences & paragraphs to a 384 dimensional dense
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

labels = ["red", "blue", "green"]

def get_values(values):
  return values.tolist()

def preprocessing(lines):
    words = lines.select(lines.text.alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', regexp_replace('word', '#', ''))
    words = words.withColumn('word', regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', regexp_replace('word', ':', ''))
    return words

def embedding(text):
      embedding =  model.encode(text)
      return embedding.tolist()

def feature_extraction(words):
    embedding_udf = udf(embedding, ArrayType(FloatType(), containsNull=False))
    words = words.withColumn("features", embedding_udf("word"))
    mode = udf(lambda vs: Vectors.dense(vs), VectorUDT())
    words = words.withColumn("features", mode("features"))


    return words

def extract(row):
    return tuple(row.features_pca.toArray().tolist()) + (row.prediction, ) 

if __name__ == "__main__":
      spark = SparkSession.builder\
      .master("local[*]")\
      .appName("Sentimental.analysis")\
      .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7')\
      .getOrCreate()

      sc = spark.sparkContext
      sc.setLogLevel("ERROR")

      schema = StructType(
        [
                StructField("text", StringType()),
                StructField("timestamp", StringType())
        ]
    )


      kmeans = KMeans(featuresCol='features',k=3)
      kmeans.setSeed(1)

      pca = PCA(k=2, inputCol='features', outputCol='features_pca')

      batch = 0

      path = "plots/"

      # Check whether the specified path exists or not
      isExist = os.path.exists(path)

      if not isExist:
            # Create a new directory because it does not exist 
            os.makedirs(path)
            print("The new directory is created!")

      while True: 

            lines = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_url) \
            .option("subscribe", topic_name) \
            .load().selectExpr("CAST(value AS STRING)") \
            .withColumn("value", from_json("value", schema)) \
            .select(col('value.*'))

            start = time.time() 

            if lines.rdd.isEmpty():
                  print("No data waiting for 30 seconds")
                  time.sleep(30)

            print("Processing data...")
            words = preprocessing(lines)
            words = feature_extraction(words)
            words = words.repartition(1)


            KMeans_fit = kmeans.fit(words)

            pca_model = pca.fit(words)

            output = KMeans_fit.transform(words)
            points = pca_model.transform(output).select('features_pca', 'prediction')

            points = points.rdd.map(extract).toDF()
            points = points.selectExpr("_1 as x", "_2 as y", "_3 as label").toPandas()

            centers = KMeans_fit.clusterCenters()

            fig = seaborn.relplot(data=points, x='x', y='y', hue='label', hue_order=labels, aspect=1.61)
            plt.savefig('plots/'+hashtags[0]+str(batch)+'.png')
            plt.clf()

            batch = batch + 1

            end = time.time()
            timeTaken = end - start
            if(timeTaken <= 30):
                  print("Waiting for next batch of data...")
                  time.sleep(30 - timeTaken)

    


