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

def cleanLine(line):
    line = line.strip().split(':')
    if line[0] not in leagueIds:
        return
    linked = line[1].strip().split(" ")
    return (line[0],linked)

lines = lines.map(cleanLine)
lines = lines.map(lambda l:(l[0],len(l[1])))
league = lines.sortBy(lambda x: x[1],ascending=True).collect()
rank = [0] * len(league)

for i in range(len(league)):
    count = 0
    for j in range(len(league)):
        if league[i][1] > league[j][1]:
            count += 1
    rank[i] = count

for i in range(len(league)):
    league[i][1] = rank[i]

league = sorted(league, key=lambda x:x[0])

output = open(sys.argv[3], "w")
for i in league:
    output.write('%s\t%s\n' % ( i[0] , i[1] ))
output.close()
sc.stop()

