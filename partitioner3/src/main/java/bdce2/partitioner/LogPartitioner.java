package bdce2.partitioner;

import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogPartitioner extends Partitioner<Text,IntWritable > implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> months = new HashMap<String, Integer>();

  /**
   * Set up the months hash map in the setConf method.
   */
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
   /* Set the key and values for the hashmap*/
    months.put("2017-12-01", 0);
    months.put("2017-12-02", 1);
    months.put("2017-12-03", 2);
    months.put("2017-12-04", 3);
    months.put("2017-12-05", 4);
    months.put("2017-12-06", 5);
    months.put("2017-12-07", 6);
    months.put("2017-12-08", 7);
    months.put("2017-12-09", 8);
    months.put("2017-12-10", 9);
    months.put("2017-12-11", 10);
    months.put("2017-12-12", 11);
    months.put("2017-12-13", 12);
    months.put("2017-12-14", 13);
    months.put("2017-12-15", 14);
    months.put("2017-12-16", 15);
    months.put("2017-12-17", 16);
    months.put("2017-12-18", 17);
    months.put("2017-12-19", 18);
    months.put("2017-12-20", 19);
    months.put("2017-12-21", 20);
    months.put("2017-12-22", 21);
    months.put("2017-12-23", 22);
    months.put("2017-12-24", 23);
    months.put("2017-12-25", 24);
    months.put("2017-12-26", 25);
    months.put("2017-12-27", 26);
    months.put("2017-12-28", 27);
    months.put("2017-12-29", 28);
    months.put("2017-12-30", 29);
    months.put("2017-12-31", 30);
  }
  
  /**
   * Implement the getConf method for the Configurable interface.
   */
  public Configuration getConf() {
    return configuration;
  }
  
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	  
	 
	if (key.getLength()>=0)
		{
		
		//Extract the theDate from the string and return the appropriate reducer number from the hashmap
		//and the key and value from the mapper goes to that partitcular reducer 
		//therefore all song ids of same date go to the same reducer
		   String compKey[] = key.toString().split("::");
		   String theDate=compKey[0];
	       return (int) (months.get(theDate));
		}
	else 
		return numReduceTasks;
	  
          
    
  }
}
