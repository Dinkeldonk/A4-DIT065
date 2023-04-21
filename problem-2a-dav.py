import findspark
findspark.init()
from math import sqrt
from pyspark import SparkContext
sc = SparkContext(master = 'local[4]')

data = sc.textFile('/data/2023-DAT470-DIT065/data-assignment-3-1M.dat')

def extract_values(line):
    id, group, value = map(float, line.split("\t"))
    return group, (value, value, 1)

def reduce_stats(a, b):
    s1, s2, c1 = a
    s3, s4, c2 = b
    return s1 + s3, s2 + s4, c1 + c2

group_stats = data.map(extract_values) \
                  .reduceByKey(reduce_stats)

result = group_stats.map(lambda x: (x[0], x[1][0] / x[1][2], 0.0 if x[1][2] <= 1 else sqrt(max(0.0, (x[1][1] / x[1][2]) - (x[1][0] / x[1][2])**2)), min(x[1][0], x[1][1]), max(x[1][0], x[1][1]), x[1][2>

for group, mean, stddev, min_val, max_val, count in result.collect():
    print("Group: {}\tMean: {}\tStandard Deviation: {}\tMinimum Value: {}\tMaximum Value: {}\tCount: {}".format(group, mean, stddev, min_val, max_val, count))

