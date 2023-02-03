from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("teenage-friends-sparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name= str(fields[1]),
               age = int(fields[2]), numFriends = int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

query = "SELECT * FROM people WHERE age >=13 AND age <=19"
teenagers = spark.sql(query)

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupby("age").count().orderBy("age").show()
spark.stop()
