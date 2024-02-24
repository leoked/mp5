#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def cleanLine(line):
    line = line.strip().split('\t')
    return int(line[1])

lines = sc.textFile(sys.argv[1], 1)
num = lines.map(cleanLine)

sum = num.reduce(lambda a, b: a + b)
length = num.count()

ans1 = sum // length
ans2 = sum
ans3 = num.min()
ans4 = num.max()
ans5 = num.variance()

outputFile = open(sys.argv[2], "w")
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % int(ans5))
outputFile.close()

sc.stop()