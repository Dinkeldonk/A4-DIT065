import findspark #type: ignore
findspark.init()
from math import sqrt
from pyspark import SparkContext #type: ignore
import math
import argparse
import time

def extract_value(line):
    _, _, value = line.split("\t")
    value = float(value)
    return value, value**2, 1, value, value

def sum_and_minmax(a, b):
    v1, s1, c1, minv1, maxv1 = a
    v2, s2, c2, minv2, maxv2 = b
    return v1 + v2, s1 + s2, c1 + c2, min(minv1,minv2), max(maxv1, maxv2)

if __name__ == '__main__':
    program_start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', '-w', default=1, type=int)
    parser.add_argument('--file_choice', '-f', type=int,
                        default=1,
                        help='An integer specifying which data file to use. \
                            One of [1,10,100,1000]',
                        choices=[1,10,100,1000])
    parser.add_argument('--timing_file', '-t', default='prob2b_results',
                        help='The filename to append timing results to.')
    args = parser.parse_args()

    sc = SparkContext(master = f'local[{args.workers}]')
    
    if args.file_choice == 1000:
        filename = f'/data/2023-DAT470-DIT065/data-assignment-3-1B.dat'
    else:
        filename = f'/data/2023-DAT470-DIT065/data-assignment-3-{args.file_choice}M.dat'
    data = sc.textFile(filename)

    val_sum, sq_sum, N, minv, maxv = data.map(extract_value).reduce(sum_and_minmax)
    mean = val_sum / N
    var = sq_sum/N - mean**2
    std = math.sqrt(var)

    program_end = time.time()
    program_time = program_end - program_start

    print('Number of workers:', args.workers)
    print(f'Total running time: {program_time:.4f}')
    print('Mean\tstd\tmin\tmax')
    print(f'{mean:.4f}\t{std:.4f}\t{minv:.4f}\t{maxv:.4f}\n')

    with open(args.timing_file, 'a') as f:
        f.write(f'{args.workers} {program_time}')
