# Implement a single step of the k-means iteration using MrJob.
# That is, implement assigning all data points to the closest centroid 
# followed by recomputing the centroids. 
#
# Assume that the mapper is applied to each data point and that 
# centroids are communicated in another way, e.g., by reading a file. 
#
# Discuss the disk access and network communication patterns of this 
# implementation, and the advantages and disadvantages of using a MRJob 
# implementation for computing k-means.

# >>> We let the file be named 'centroids' and contain a string representation
# >>> of the current centroids. 



from mrjob.job import MRJob, MRStep
import logging
import numpy as np

def nearestCentroid(datum, centroids):
    """computes the distance of the data vector to the centroids and returns 
    the closest one as an (index,distance) pair
    """
    # norm(a-b) is Euclidean distance, matrix - vector computes difference
    # for all rows of matrix
    dist = np.linalg.norm(centroids - datum, axis=1)
    return np.argmin(dist), np.min(dist)

def kmeans(k, data, nr_iter = 100):
    """computes k-means clustering by fitting k clusters into data
    a fixed number of iterations (nr_iter) is used
    you should modify this routine for making use of multiple threads
    """
    N = len(data)

    # Choose k random data points as centroids
    centroids = data[np.random.choice(np.array(range(N)),size=k,replace=False)]
    logging.debug("Initial centroids\n", centroids)

    N = len(data)

    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    logging.info("Iteration\tVariation\tDelta Variation")
    total_variation = 0.0
    for j in range(nr_iter):
        logging.debug("=== Iteration %d ===" % (j+1))

        # Assign data points to nearest centroid
        variation = np.zeros(k)
        cluster_sizes = np.zeros(k, dtype=int)        
        for i in range(N):
            cluster, dist = nearestCentroid(data[i],centroids)
            c[i] = cluster
            cluster_sizes[cluster] += 1
            variation[cluster] += dist**2
        delta_variation = -total_variation
        total_variation = sum(variation) 
        delta_variation += total_variation
        logging.info("%3d\t\t%f\t%f" % (j, total_variation, delta_variation))

        # Recompute centroids
        centroids = np.zeros((k,2)) # This fixes the dimension to 2
        for i in range(N):
            centroids[c[i]] += data[i]        
        centroids = centroids / cluster_sizes.reshape(-1,1)
        
        logging.debug(cluster_sizes)
        logging.debug(c)
        logging.debug(centroids)
    
    return total_variation, c

line = '[8.6504758  4.88295791]'

class MRkmean(MRJob):

    def mapper(self, _, line):
        datap = np.fromstring(line[1:-1], dtype=np.float, sep=' ')

        pass

    #def combiner():
    #    pass

    def reducer(self, _, values):
        pass

if __name__ == '__main__':
    MRkmean.run()