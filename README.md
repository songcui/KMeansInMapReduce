KMeansInMapReduce
=================

This project implements K-means clustering algorithm in MapReduce, Hadoop 2.0. The purpose of this project is not to provide an alternative version of K-Means clustering algorithms in Apache Mahout. Instead, this project aims to help people who are not using Mahout, Spark or other Hadoop machine learning libraries to implement K-means in their Hadoop projects. The standard version of K-means is implemented and the mathematics of the algorithm can be found in http://en.wikipedia.org/wiki/K-means_clustering . 

In this project, one map-reduce job is designed for a single k-means iteration. In order to run multiple k-means iterations, I design chained map-reduce jobs: "map 1-> reduce 1-> map 2 -> reduce 2 -> ...... -> map n -> reduce n" by using "JobControl" to schedule the jobs. 

The project contains 2 java files. KMeans.java contains the main method and MapReduce jobs. CentroidInitialization.java reads the file contains the centroids coordinates. 

There are 3 arguments need to be provided by the user in the following sequence: "File directory for data points to be clustered", "File directory for initial centroids", "An integer specifies the number of iterations". The file containing the data points should be in txt format or other formats can be read by map method in Mapper class in Hadoop. Each data point is in one line and each dimension of the data point is separated by space. The similar requirement is for the file containing initial centroids as well. There is no need to specify the number of centroids as the code will capture this information automatically from the file containing the initial centroids. As an example, the file contains 5D data points and initial centroids should look like this:

0 0.64 0.64 0 0.32    
0.21 0.28 0.5 0 0.14 
.......


   
