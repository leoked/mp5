#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 10)
leagueIds = sc.textFile(sys.argv[2], 1)
leagueIds = leagueIds.collect()
lid = sc.broadcast(leagueIds)

def cleanLine(line):
    line = line.strip().split(':')
    linked = line[1].strip().split(" ")
    return linked

links = lines.flatMap(cleanLine)
links = links.map(lambda l: (l, 1)).reduceByKey(lambda a, b: a + b)
league = links.filter(lambda x:x[0] in lid.value).collect()
league = list(league)
rank = [0] * len(league)

for i in range(len(league)):
    count = 0
    for j in range(len(league)):
        if league[i][1] > league[j][1]:
            count += 1
    rank[i] = count

for i in range(len(league)):
    rank[i] = [league[i][0],rank[i]]

rank = sorted(rank, key=lambda x:x[0])

output = open(sys.argv[3], "w")
for i in rank:
    output.write('%s\t%s\n' % ( i[0] , i[1] ))
output.close()
sc.stop()
