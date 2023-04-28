import findspark #type: ignore
findspark.init()
from math import sqrt
from pyspark import SparkContext #type: ignore
import math
import argparse

def extract_value(line):
    _, _, value = line.split("\t")
    value = float(value)
    return value, 1

def count_points(a, b):
    _, c1 = a
    _, c2 = b
    return None, c1 + c2

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', '-w', default=1, type=int)
    parser.add_argument('--file_choice', '-f', type=int,
                        default=1,
                        help='An integer specifying which data file to use. \
                            One of [1,10,100,1000]',
                        choices=[1,10,100,1000])
    args = parser.parse_args()
    
    sc = SparkContext(master = f'local[{args.workers}]')

    if args.file_choice == 1000:
        filename = f'/data/2023-DAT470-DIT065/data-assignment-3-1B.dat'
    else:
        filename = f'/data/2023-DAT470-DIT065/data-assignment-3-{args.file_choice}M.dat'
    data = sc.textFile(filename)

    _, N = data.map(extract_value).reduce(count_points)
    """
    mean = val_sum / N
    var = sq_sum/N - mean**2
    std = math.sqrt(var)

    print('Mean\tstd\tmin\tmax')
    print(f'{mean:.4f}\t{std:.4f}\t{minv:.4f}\t{maxv:.4f}')
    """
    sorted_vals = data.map(extract_value).sortBy(lambda v: v[0]).collect()

    #sorted_vals = sc.parallelize(data).sortBy(lambda v: v[2]).collect()

    #print(sorted_vals)

    if N % 2 == 0:
        med = (sorted_vals[N//2 - 1][0] + sorted_vals[N//2][0]) / 2
        #med = (sorted_vals[N//2 - 1][2] + sorted_vals[N//2][2]) / 2
    else:
        med = sorted_vals[N//2][0]
        #med = sorted_vals[N//2][2]

    print("Median: ", med)
