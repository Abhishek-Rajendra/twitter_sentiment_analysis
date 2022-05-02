from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.mllib.recommendation import ALS, Rating

spark = SparkSession.builder\
    .master("local")\
    .appName("Colab")\
    .config('spark.ui.port', '4050')\
    .getOrCreate()


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# Load and parse the data

data = sc.textFile("ratings.dat")
ratings = data.map(lambda l: l.split('::'))\
    .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

ratings = ratings.map(lambda x: (x[0], x[1], x[2]))
train, test = ratings.randomSplit([6, 4], seed=0)
prediction_test = test.map(lambda x: (x[0], x[1]))

# Build the recommendation model using Alternating Least Squares

seed = 10
iterations = 10
ranks = [4, 8, 10]
errors = {}
lambdaValue = 0.1

print("Regularization parameter.: ", lambdaValue)

for rank in ranks:
    print("Rank:", rank)
    model = ALS.train(train, rank, iterations=iterations, lambda_=lambdaValue, seed=seed)
    predictions = model.predictAll(prediction_test).map(lambda r: (((r[0], r[1]), r[2])))
    ratesAndPreds = test.map(lambda r: (((int(r[0]), int(r[1])), float(r[2])))).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    errors[rank] = MSE
    print("Mean Squared Error = " + str(MSE))

        
best = min(errors.items(), key=lambda x: x[1]) 

print("Best MSE for Rank %d :- %f" % best)
