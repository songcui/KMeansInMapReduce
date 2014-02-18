
/* This code reads the intial centroids from a txt file where one centroid is specified in one line. Each dimension
of a centroid is separated by space. */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CentroidInitialization{
	public static ArrayList <String> centroidInitialization(String centroidDirFile){
	   ArrayList <String> centroidMatrix  = new ArrayList<String>();
	   
	   try{
		   BufferedReader centroid_read = new BufferedReader(new FileReader(centroidDirFile));
		   while(true){
			   String line = centroid_read.readLine();
			   if (line == null){
				   break;
			   }
			   else centroidMatrix.add(line);
			   }
		   centroid_read.close();
	   }
	  
	   catch(IOException e){
		   System.out.println("No such a file!");
	   }
	return centroidMatrix;
	  }
}
