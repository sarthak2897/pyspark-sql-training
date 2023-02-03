from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col,round

spark = SparkSession.builder.appName("total-amount-by-id-df").getOrCreate()
schema = StructType([StructField("customerId",StringType(),True),
                     StructField("orderId",StringType(),True),
                     StructField("amount",FloatType(),True)])

lines = spark.read.schema(schema).csv("customer-orders.csv")

amountById = lines.select("customerId","amount").groupby("customerId").sum("amount")\
    .withColumnRenamed("sum(amount)","amount").sort("amount",ascending=False)

amountById.show()

amountByIdSorted = amountById.withColumn("amount",round(col("amount"),2)).select("customerId","amount")
for amount in amountByIdSorted.collect():
    print(amount[0], amount[1])

spark.stop()