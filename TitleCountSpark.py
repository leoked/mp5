#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

def cleanLine(line,stopWords):
    line = line.strip().lower()
    for c in delimiter:
        line = line.replace(c, ' ')
    line = line.replace('  ', ' ')
    words = line.split(' ')
    words = [word for word in words if word not in stopWords]
    return words

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	stopWords = f.read().split('\n')

with open(delimitersPath) as f:
    delimiter = f.read()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
words = lines.flatMap(cleanLine)
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
sorted_word_counts = word_counts.sortBy(lambda word_count: word_count[1], ascending=False)

print(sorted_word_counts)
## outputFile = open(sys.argv[4],"w")



sc.stop()
