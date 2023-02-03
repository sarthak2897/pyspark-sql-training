from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("max-temp")
sc = SparkContext(conf=conf)

def parseTemp(line):
    fields = line.split(',')
    stationId = fields[0]
    type = fields[2]
    temp = float(fields[3])
    return stationId, type, temp


maxString = "TMAX"
lines = sc.textFile("1800.csv")
filterMax = lines.map(parseTemp).filter(lambda x: maxString in x[1])
maxTemp = filterMax.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y))
maxTempList = maxTemp.collect()
for maxTemp in maxTempList:
    print(maxTemp)
