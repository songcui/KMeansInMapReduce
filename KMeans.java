import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.lang.Runnable;
import javax.naming.NamingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;           
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;          
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;



public class KMeans extends Configured implements Tool {
	public static int DIMENSION = 58;
	/* The dimension of the data points to be clustered */
	
	
	public static void main(String[] args) throws Exception {
	  if (args.length!=3){
		  System.out.println("Usage: KMeans <data dir> <centroid dir> <Number of iterations> \n");
		  System.exit(-1);
	  } 
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new KMeans(), args);
      
      System.exit(res);
   }
   
    public int run(String[] args) throws Exception {
	   System.out.println(Arrays.toString(args));
	   int number_of_iteractions = Integer.parseInt(args[2]);

	   String FileDir = "Output";
	   String FileDirFile = "/part-r-00000";
	  
	   JobControl control = new JobControl("KMeansMapReduce");
	   
	   ControlledJob step0 = new ControlledJob(KmeansMapReduce(args[0], FileDir+"0", args[1]), null);
	   control.addJob(step0);

	   if (number_of_iteractions>1){
		   HashMap<Integer, ControlledJob> chainJobNames= new HashMap<Integer, ControlledJob>();
		   for (int i=0; i<number_of_iteractions-1; i++){
			   if (i!=0){
			   chainJobNames.put(i, new ControlledJob(KmeansMapReduce(args[0], FileDir+String.valueOf(i+1), FileDir+String.valueOf(i)+FileDirFile), Arrays.asList(chainJobNames.get(i-1))));
			   }
			   else {
				   chainJobNames.put(i, new ControlledJob(KmeansMapReduce(args[0], FileDir+"1", FileDir+"0"+FileDirFile), Arrays.asList(step0)));
			   }
		   }
	   }
	   
	   /* The codes above in run method schedule chained jobs. Each "KmeansMapReduce" MapReduce job
	   runs 1 iteraction of K-means clustering. The job dependency needs to be specified so that the next
	   iteraction only start to run after the current iteraction finishes.*/
	   
	   Thread workFlowThread =  new Thread(control, "workflowthread");
	   workFlowThread.setDaemon(true);
	   workFlowThread.start();  
	   
	   /* put the chained MapReduce jobs in a thread and run*/
	   
	   return 0;
   }

    public Job KmeansMapReduce(String inputPath, String outputPath, String centroidDirFile) throws IOException, InterruptedException, ClassNotFoundException{	   
       
       Configuration conf = new Configuration();	
	   ArrayList<String> centroid = CentroidInitialization.centroidInitialization(centroidDirFile);
       for (int i=0;i<centroid.size();i++){
		   conf.set(String.valueOf(i), centroid.get(i));
	   }
	   conf.set("NumberOfCentroid", String.valueOf(centroid.size()));
	   
	   /* The codes above stores the centroids to input to K-means clustering algorithms in the values of 
	   conf<key, value>. We can read the centroids in the setup method in Mapper class later on.*/
    	
       @SuppressWarnings("deprecation")
       Job job = new Job(conf, "job configuration");
	   job.setJarByClass(KMeans.class);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(NullWritable.class);

	   job.setMapperClass(KMeansMap.class);
	   job.setMapOutputKeyClass(IntWritable.class);  
	   job.setMapOutputValueClass(Text.class);
	   job.setReducerClass(KMeansReduce.class);

	   job.setInputFormatClass(TextInputFormat.class);   
	   job.setOutputFormatClass(TextOutputFormat.class); 

	   FileInputFormat.addInputPath(job, new Path(inputPath));
	   FileOutputFormat.setOutputPath(job, new Path(outputPath));
	   
	   job.waitForCompletion(true);
	   
	   /* The codes above specify various formats of the job */ 

	   return job;
   }

   

   public static class KMeansMap extends Mapper<LongWritable, Text, IntWritable, Text> {
	   private HashMap<String, String> centroid_register = new HashMap<String, String> ();
	   private ArrayList<ArrayList<Double>>centroid_array = new ArrayList<ArrayList<Double>>();
       	   private int numberOfDimension;
           private ArrayList<Double>data_point = new ArrayList <Double>();
	   private IntWritable clusterNumber = new IntWritable();

	   
           public void setup (Context context) throws IOException,
		InterruptedException {
			 	  	
   	  	/* The setup method reads the centroid data by retrieving the value from conf<key, value>. The
   	  	centroid data are then left in the memory and the map method can read the centroid data  in order
   	  	assign the nearest centroid to each data point.*/
   	  	
    	          String k = "NumberOfCentroid";
		  for (int i=0;i<Integer.parseInt(context.getConfiguration().get(k));i++){
		   centroid_register.put(String.valueOf(i), context.getConfiguration().get(String.valueOf(i)));
		  }
		  
   	  	  for (int i=0; i<centroid_register.size(); i++){
   	  		String rowString = centroid_register.get(String.valueOf(i)).toString();
   	  		ArrayList<Double> centroid_row = new ArrayList<Double>();
   	  		for (String elementString: rowString.split("\\s+")){
   	  			centroid_row.add(Double.parseDouble(elementString));
   	  		}
   	  		centroid_array.add(centroid_row);
		  }
   	  	numberOfDimension = centroid_array.get(0).size();
   	  	if (DIMENSION != numberOfDimension){
   	  	DIMENSION = numberOfDimension; 
   	  	System.out.println("Dimension is not correct");
   	  	}
   	  	else System.out.println("Dimension is correct");
	  }	
		     
   	 @Override
       public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException{ 

    	  	for (String element_data: value.toString().split("\\s+")){
    	  		data_point.add(Double.parseDouble(element_data));
    	  	}
    	  	
    	  	if (data_point.size()!= DIMENSION){
    	  		System.out.println("Dimension of the datapoint is not correct!");
    	  	}
    	  	
    	  	double sumError = 0;
    	  	int clusterAssignment = 0;
    	  	double norm_space = 2;
    	  	for (int i=0; i<centroid_array.size();i++){
    	  		double sum = 0;
    	  		for (int j=0;j<data_point.size();j++){
    	  			double intermediate_value = data_point.get(j)-centroid_array.get(i).get(j);
    	  			sum += Math.pow(intermediate_value, norm_space);    	  		
    	  		}
    	  		if (i==0){
    	  			sumError = sum;
    	  		}
    	  		else if (sum<sumError){
    	  			clusterAssignment = i;
    	  			sumError = sum;
    	  		}    	  		
    	  	}
    	  	clusterNumber.set(clusterAssignment);
    	  	context.write(clusterNumber, value);
    	  	data_point.clear();
   	 }
     public void cleanup(javax.naming.Context context) throws IOException, InterruptedException, NamingException{
     	 context.close();
    }
   }
   public static class KMeansReduce extends Reducer<IntWritable, Text, Text, NullWritable> {
	  @Override
      public void reduce(IntWritable key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
         Text newCentroid = new Text();
         String newCentroidString = "";
         Double sum[] = new Double[DIMENSION];
         for (int j=0; j<DIMENSION; j++){
        	 sum[j]=(double) 0;
         }
         
         int denominator = 0;
         for (Text val : value) {
        	 int i = 0;
        	 denominator+=1;
        	 for (String datapointReduce: val.toString().split("\\s+")){
        		 if (i<DIMENSION){
        			 if (datapointReduce!=null){
        				 sum[i]+=Double.parseDouble(datapointReduce);
        				 i+=1;
        			 }
        			 else sum[i]+=0;
        		 }
        	 }
         }
         
         for (int i =0; i<DIMENSION; i++){
        	 newCentroidString+=String.valueOf(sum[i]/denominator);
        	 if (i!=DIMENSION-1){
        		 newCentroidString+=" ";
        	 }
         }
         newCentroid.set(newCentroidString);
         context.write(newCentroid, NullWritable.get());
   }
    public void cleanup(javax.naming.Context context) throws IOException, InterruptedException, NamingException{
     	 context.close();
    }
  	   
}
  }

 
   
