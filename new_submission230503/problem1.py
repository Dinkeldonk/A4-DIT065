from mrjob.job import MRJob

class MRKmeans(MRJob):

    def configure_args(self):
        super(MRKmeans, self).configure_args()
        self.add_file_arg('-centroids', help='centroid file')    

    def get_centroids(self):
        """Returns the current centroids from disk."""
        centroids = []
        with open(self.options.centroids, 'r') as f:
            for line in f:
                centroid = [float(x) for x in line.strip().split()]
                centroids.append(centroid)
        return centroids
 
    def mapper_init(self):
        """Initializes self.centroids with the centroids on disk."""
        self.centroids = self.get_centroids()

    def mapper(self, _, line):
        """Yields the index of the closest centroid and the 
        point itself from every line."""
        point = list(map(float, line.strip().split()))
        centroids = self.centroids
        min_distance = float('inf')
        closest_centroid = None

        for i, centroid in enumerate(centroids):
            distance = sum([(point[j] - centroid[j])**2 for j in range(len(point))])
            if distance < min_distance:
                min_distance = distance
                closest_centroid = i

        yield closest_centroid, point


    def reducer(self, centroid_id, points):
        """Takes a centroid id and its associated points and
        yields the centroid id and its new position."""
        num_points = 0
        #centroids = self.get_centroids()
        new_centroid = [0.0, 0.0] 
        for point in points:
            num_points += 1
            for i in range(len(point)):
                new_centroid[i] += point[i]
        new_centroid = [x / num_points for x in new_centroid]


        yield centroid_id, new_centroid

if __name__ == "__main__":
    MRKmeans.run()
