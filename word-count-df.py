from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,lower

spark = SparkSession.builder.appName("word-count-df").getOrCreate()

lines = spark.read.text("Book.txt")
lines.printSchema()

words = lines.select(explode(split(lines.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercaseWords = words.select(lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupby("word").count().sort("count",ascending=False)

wordCounts.show(wordCounts.count())