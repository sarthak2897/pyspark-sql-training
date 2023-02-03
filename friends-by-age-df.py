from pyspark.sql import SparkSession, Row , functions

spark = SparkSession.builder.appName("friends-by-age-df").getOrCreate()

friends = spark.read.option("header","true").option("inferSchema","true")\
    .csv("fakefriends-header.csv")

friendsByAge = friends.select("age","friends")
#friendsByAge.groupby("age").avg("friends").sort("age").show()

friendsByAge.groupby("age")\
    .agg(functions.round(functions.avg("friends"),2)\
    .alias("friends_avg")).sort("age").show()

spark.stop()
