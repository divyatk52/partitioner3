package bdce2.partitioner;
/**
* 
* The "LogDriver" program holds the driver code to create a job instance
* for which a mapper ,combiner ,partitioner and reducer classes are created
* to perform a total count of songs played date-wise
* and to get top 500 songs date-wise.
*  
*/

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;



public class LogDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new LogDriver(), args);
        System.exit(returnStatus);
    }



public int run(String[] args) throws IOException{



	Configuration conf;
	Job job;
	conf = new Configuration();
	job = Job.getInstance(conf, "saavn_b");
	 job.setJarByClass(LogDriver.class);
	 
	 // the output key,value is set to Text(date::sid),IntWritable(count)
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 
	 
	 job.setMapperClass(Mapper1.class);
	 job.setCombinerClass(LogCombiner.class);
	 job.setPartitionerClass(LogPartitioner.class);
	 job.setReducerClass(LogReducer.class);
	 job.setNumReduceTasks(31);//partitioned date wise for the 31 days of December.
	 
	 
	//Input and Output paths are set.
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	   	
	
	try {
		return job.waitForCompletion(true) ? 0 : 1;
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return 0;
}


}
