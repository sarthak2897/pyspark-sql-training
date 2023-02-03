from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, size, desc
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("most-popular-superhero").getOrCreate()

schema = StructType([StructField("id",IntegerType(),True),
                     StructField("name",StringType(),True)])

names = spark.read.option("sep"," ").schema(schema).csv("MarvelNames.csv")

names.printSchema()

lines = spark.read.text("MarvelGraph.txt")

lines.printSchema()

heroConnections = lines.withColumn("id",split(col("value")," ")[0])\
    .withColumn("connections",size(split(col("value"), " ")) - 1).groupby("id").sum("connections")\
    .withColumnRenamed("sum(connections)","total_connections").sort(desc("total_connections")).first()

names.filter(col("id") == heroConnections.id).select("id","name").show()
