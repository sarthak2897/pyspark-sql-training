from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("word-count")
sc = SparkContext(conf=conf)

def normalizeWords(line):
    return re.compile(r"\W+", re.UNICODE).split(line.lower())

lines = sc.textFile("Book.txt")
splitWords = lines.flatMap(normalizeWords)
wordCount = splitWords.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
wordCountSorted = wordCount.map(lambda x: (x[1],x[0])).sortByKey(False)
sortedList = wordCountSorted.collect()
for result in sortedList:
    count = str(result[0])
    print(result[1],count)
    #cleanword = result[1].encode("ascii", "ignore")
    #if(cleanword):
        #print(cleanword + ":\t\t" + count)
