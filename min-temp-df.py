from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("min-temp-df").getOrCreate()

schema = StructType([StructField("stationId",StringType(),True),
                     StructField("date",IntegerType(),True),
                     StructField("measure_type",StringType(),True),
                     StructField("temperature",FloatType(),True)])

lines = spark.read.schema(schema).csv("1800.csv")
lines.printSchema()
minString = "TMIN"

minTemp = lines.filter(lines.measure_type == minString)\
    .select("stationId","temperature").groupby("stationId")\
    .min("temperature").sort("min(temperature)")

minTemp.show()

minTempByStation = minTemp.withColumn("temperature",
                                      round(col("min(temperature)"),2))\
    .select("stationId","temperature").sort("temperature")

results = minTempByStation.collect()
for result in results:
    print(result[0], result[1])

spark.stop()