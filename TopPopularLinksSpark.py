#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 10) 

def cleanLine(line):
    line = line.strip().split(':')
    linked = line[1].strip().split(" ")
    return linked

links = lines.flatMap(cleanLine)
links = links.map(lambda l: (l, 1)).reduceByKey(lambda a, b: a + b)
links = links.filter(lambda x:x[1] > 400)
links = links.sortBy(lambda x:x[1], ascending=False)
links = links.take(10)
tLinks = sorted(links, key=lambda x:x[0])

output = open(sys.argv[2], "w")
for link, count in tLinks:
    output.write('%s\t%s\n' % ( link , count ))
output.close()

sc.stop()

