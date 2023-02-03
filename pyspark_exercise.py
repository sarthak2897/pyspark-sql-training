from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, round, isnan, when, avg, month
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DoubleType, \
    BooleanType

spark = SparkSession.builder.appName("mutual-funds").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

schema = StructType([StructField("schemeCode",IntegerType(),False),
                     StructField("schemeName",StringType(),False),
                     StructField("isinGrowth",StringType(),False),
                     StructField("isinDivReinvestment",StringType(),False),
                     StructField("nav",DoubleType(),False),
                     StructField("repurchasePrice",DoubleType(),False),
                     StructField("salePrice",DoubleType(),False),
                     StructField("date",DateType(),False),
                     StructField("corrupt_record",StringType(),False)])

# schema = StructType([StructField("Scheme Code",IntegerType(),False),
#                      StructField("Scheme Name",StringType(),False),
#                      StructField("ISIN Div Payout/ISIN Growth",StringType(),False),
#                      StructField("ISIN Div Reinvestment",StringType(),False),
#                      StructField("Net Asset Value",DoubleType(),True),
#                      StructField("Repurchase Price",DoubleType(),True),
#                      StructField("Sale Price",DoubleType(),True),
#                      StructField("Date",DateType(),False),
#                      StructField("corrupt_record",BooleanType(),True)])

lines = spark.read.option("header", "true").schema(schema).csv("DailyNAV")
    # .option("mode", "PERMISSIVE")\
    # .option("columnNameOfCorruptRecord","corrupt_record")\
#\\data_2006_04_01.csv
#2021_11_18

lines.printSchema()

filteredDf1 = lines.withColumn("corrupt_record",when(col("repurchasePrice").isNotNull(),False).otherwise(True))
filteredDf2 = filteredDf1.withColumn("corrupt_record",when(col("nav").isNotNull(),False).otherwise(True))
filteredDf3 = filteredDf2.withColumn("corrupt_record",when(col("salePrice").isNotNull(),False).otherwise(True))

filterZeros = filteredDf3.filter((col("nav") != 0.0) | (col("repurchasePrice") != 0.0) | (col("salePrice") != 0.0) )

transformDf = filterZeros.withColumn("date",date_format(col("date"),"yyyy-MM-dd"))\
    .withColumn("exitLoad",round((col("nav") * 0.01),2))

#transformDf.filter(col("corrupt_record")).drop(col("corrupt_record")).write.csv("error_data\\error")

# print(transformDf.filter(col("corrupt_record")).count())
# print(transformDf.filter(~col("corrupt_record")).count())

filterCorruptDf = transformDf.filter(col("corrupt_record") == False)

averageDf = filterCorruptDf.groupby("schemeCode",month("date").alias("month")).agg(round(avg("nav"),2).alias("avg_nav"),round(avg("repurchasePrice"),2).alias("avg_repurchasePrice"),
                                                 round(avg("salePrice"),2).alias("avg_salePrice")).orderBy("month")

#averageDf.show(averageDf.count())

nonZeroSchemeCode = filterCorruptDf.select(col("schemeCode")).where(~(col("exitLoad") == 0.0))

