KMeansInMapReduce
=================

This project implements K-means clustering algorithm in MapReduce, Hadoop 2.0. The purpose of this project is not to provide an alternative version of K-Means clustering algorithms in Apache Mahout. Instead, this project aims to help people who are not using Mahout, Spark or other Hadoop machine learning libraries to implement K-means in their Hadoop projects. The standard version of K-means is implemented and the mathematics of the algorithm can be found in http://en.wikipedia.org/wiki/K-means_clustering . 

In this project, one map-reduce job is designed for a single k-means iteration. In order to run multiple k-means iterations, I design chained map-reduce jobs: "map 1-> reduce 1-> map 2 -> reduce 2 -> ...... -> map n -> reduce n" by using "JobControl" to schedule the jobs. 

The project contains 2 java files. KMeans.java contains the main method and MapReduce jobs. CentroidInitialization.java reads the file contains the centroids coordinates. 

Th
      
