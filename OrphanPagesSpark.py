#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def cleanLine(line):
    line = line.strip().split('\t')
    page = int(line[0])
    linked = line[1].strip().split(" ")
    linked = [int(i) for i in linked]
    return (page, linked)

lines = sc.textFile(sys.argv[1], 10) 
lines = lines.map(cleanLine)
linked = lines.flatMap(lambda x:x[1]).distinct()
page = lines.map(lambda x:x[0]).distinct()
orphan = page.subtract(linked).collect()

output = open(sys.argv[2], "w")
for o in orphan:
    output.write('%s\n' % (o))
output.close()

sc.stop()

