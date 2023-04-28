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

# DOES NOT WORK!

from mrjob.job import MRJob, MRStep
import logging
import numpy as np



class MRKmeans(MRJob):

    def nearestCentroid(self, datum, centroids):
        """computes the distance of the data vector to the centroids and returns 
        the closest one as an (index,distance) pair
        """
        # norm(a-b) is Euclidean distance, matrix - vector computes difference
        # for all rows of matrix
        dist = np.linalg.norm(centroids - datum, axis=1)
        return np.argmin(dist), np.min(dist)
    
    def configure_args(self):
        super(MRKmeans, self).configure_args()
        self.add_file_arg('-centroids', help='centroid file')
    
    def get_centroids(self):

        centroids = []
        with open(self.options.centroids, 'r') as f:
            for line in f:
                centroid = [float(x) for x in line.strip().split()]
                centroids.append(centroid)
        return centroids
    
    def mapper_init(self):
        with open(self.options.centroids, 'r') as f:
            centroids_text = f.readlines()
            self.num_centroids = len(centroids_text)
            centroids = np.zeros(self.num_centroids, 2)
            for i, centroid in enumerate(centroids_text):
                centroid = centroid.strip().split()
                centroids[i][0] = float(centroid[0])
                centroids[i][1] = float(centroid[1])
        self.centroids = centroids
 
    def mapper(self, _, line):
        datap = np.fromstring(line, dtype=np.float, sep=' ')
        cluster, dist = self.nearestCentroid(datap, self.centroids)
        yield cluster, dist**2

    def combiner(self, cluster_id, squared_dists):
        #variation = sum(squared_dists)
        cluster_size = np.zeros(self.num_centroids, dtype=int)  
        for i, (cluster, dist) in enumerate(clusters_and_dists):
            self.c[i] = cluster
            cluster_sizes[cluster] += 1
            #variation[cluster] += dist**2
        yield cluster_id, ()

    def reducer(self, _, values):
        pass

    def steps(self):
        return [
            mapper = self.mapper_init,
        ]


if __name__ == '__main__':
    MRKmeans.run()