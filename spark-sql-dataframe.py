from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema","true")\
    .csv("fakefriends-header.csv")

people.printSchema()

people.select("name","age").show()

people.filter(people.age < 21).show()

people.groupby("age").count().show()

people.select(people.name, people.age + 10).show()

spark.stop()