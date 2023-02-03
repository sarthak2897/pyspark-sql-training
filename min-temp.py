from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("min-temp")
sc = SparkContext(conf = conf)

def parseTemp(line):
    fields = line.split(',')
    stationId = fields[0]
    type = fields[2]
    temp = float(fields[3])
    return stationId, type, temp


minString = "TMIN"
lines = sc.textFile("1800.csv")
rdd = lines.map(parseTemp).filter(lambda x: minString in x[1])
groupByTemp = rdd.map(lambda x: (x[0],x[2])).reduceByKey(lambda x,y: min(x,y))
minTempByIdList = groupByTemp.collect()
for temp in minTempByIdList:
    print(temp[0]+ ":" +"{:.2f}".format(temp[1]))