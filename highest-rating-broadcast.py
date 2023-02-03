from pyspark.sql import SparkSession
import io

from pyspark.sql.functions import udf, col, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


def loadMovieNames():
    movieNames = {}
    file = io.open("ml-100k\\u.item", "r")
    for f in file:
        fields = f.split("|")
        movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("highest-rating-broadcast").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())
schema = StructType([StructField("userId", StringType(), True),
                     StructField("movieId", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])

moviesDf = spark.read.option("sep", "\t").schema(schema).csv("ml-100k\\u.data")
movieCounts = moviesDf.groupby("movieId").count()


def lookupName(movieId):
    return nameDict.value[movieId]

lookupNameUdf = udf(lookupName)

moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUdf(col("movieId")))
moviesWithNames.orderBy(desc("count")).show(10)
