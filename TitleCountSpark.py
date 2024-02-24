#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopWords = f.read().split('\n')

with open(delimitersPath) as f:
    delimiter = f.read()

stopWords.append('')
def cleanLine(line):
    line = line.strip().lower()
    for c in delimiter:
        line = line.replace(c, ' ')
    line = line.replace('  ', ' ')
    words = line.split(' ')
    words = [word for word in words if word not in stopWords]
    return words

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3])
words = lines.flatMap(cleanLine)
words = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
words = words.filter(lambda x:x[1] > 500)
words = words.sortBy(lambda x:x[1], ascending=False)
wList = words.take(10)
wList = sorted(wList, key=lambda x:x[0])

outputFile = open(sys.argv[4],"w")

for word, count in wList:
    outputFile.write('%s\t%s\n' % ( word , count ))

outputFile.close()

sc.stop()
                                      