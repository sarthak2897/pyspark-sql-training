from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("highest-rating").getOrCreate()

schema = StructType([StructField("userId",StringType(),True),
                     StructField("movieId",StringType(),True),
                     StructField("rating",IntegerType(),True),
                     StructField("timestamp",IntegerType(),True)])

lines = spark.read.option("sep","\t").schema(schema).csv("ml-100k\\u.data")
lines.printSchema()

highestRating = lines.groupby("movieId").count().orderBy(desc("count"))
highestRating.show(10)
spark.stop()