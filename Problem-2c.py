import findspark #type: ignore
findspark.init()
from math import sqrt
from pyspark import SparkContext #type: ignore
import math
import argparse

def extract_value(line):
    _, _, value = line.split("\t")
    value = float(value)
    return value, value, value

def sum_and_minmax(a, b):
    v1, minv1, maxv1 = a
    v2, minv2, maxv2 = b
    return v1 + v2, min(minv1,minv2), max(maxv1, maxv2)

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

    #Calculates min and max values of the data
    _, minv, maxv = data.map(extract_value).reduce(sum_and_minmax)

    print('Min\tMax')
    print(f'{minv:.4f}\t{maxv:.4f}')

    #Calculate intervals of 10 bins
    step = (maxv - minv) / 10
    bins = [0]*10

    #Takes every value in data and computes to which index each value belongs, adds by one for each index
    bin_count = data.map(extract_value).map(lambda t: int((t[0]-minv) // step)) \
        .map(lambda bin_index: (bin_index, 1)) \
        .reduceByKey(lambda c1, c2: c1 + c2)

    #Creates list with counts of amount of values to each bin in histogram
    for bin_index, count in bin_count.collect():
        bins[bin_index] = count

    #List of bin counts
    print("Bins: ", bins)
