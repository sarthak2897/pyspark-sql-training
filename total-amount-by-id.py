from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("total-amount-by-id")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amount = float(fields[2])
    return customerId, amount

lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine).reduceByKey(lambda x,y: x+y)
amountList = rdd.map(lambda x: (x[1],x[0])).sortByKey(False).collect()
for amount in amountList:
    print(amount[1], "{:.2f}".format(amount[0]))

