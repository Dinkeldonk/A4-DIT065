import findspark
findspark.init()
from math import sqrt
from pyspark import SparkContext
import math

sc = SparkContext(master = 'local[4]')

data = sc.textFile('/data/2023-DAT470-DIT065/data-assignment-3-1M.dat')

def extract_values(line):
    _, _, value = line.split("\t")
    value = float(value)
    return value, value**2, 1, value, value

def sum_vals(a, b):
    v1, s1, c1, minv1, maxv1 = a
    v2, s2, c2, minv2, maxv2 = b
    return v1 + v2, s1 + s2, c1 + c2, min(minv1,minv2), max(maxv1, maxv2)

sums_and_count = data.map(extract_values).reduce(sum_vals)

print(sums_and_count)
val_sum, sq_sum, N, minv, maxv = sums_and_count 
mean = val_sum / N
var = sq_sum/N - mean**2
std = math.sqrt(var)

print('Mean\tstd\tmin\tmax')
print(f'{mean:.4f}\t{std:.4f}\t{minv:.4f}\t{maxv:.4f}')
