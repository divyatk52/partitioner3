package bdce2.partitioner;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	 
	// The input key is Object
    // The input value is Text, we are handling Text Records
    // The output key is Text
    // The output value is IntWritable
	
	/**
	   * Example input line:
	   * V-HvGNCt,477552b6e41394619569fbfb9a590d5b,1512143128,15,2017-12-01
	   * songid,userid,timestamp,hour,date
	   *
	   */
	
	public static List<String> Dates = Arrays.asList("2017-12-01","2017-12-02","2017-12-03","2017-12-04","2017-12-05","2017-12-06","2017-12-07","2017-12-08","2017-12-09","2017-12-10","2017-12-11","2017-12-12","2017-12-13","2017-12-14","2017-12-15","2017-12-16","2017-12-17","2017-12-18","2017-12-19","2017-12-20","2017-12-21","2017-12-22","2017-12-23","2017-12-24","2017-12-25","2017-12-26","2017-12-27","2017-12-28","2017-12-29","2017-12-30","2017-12-31");
	public static List<String> hr = Arrays.asList("00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23");
  
  @Override
  public void map(Object key, Text value, Context con)
      throws IOException, InterruptedException {
	  
    //Mapper takes each record as value
    String[] fields = value.toString().split(",");//split input line by ","
    
    if (fields.length > 4) {
    	
      //first field of input is assigned to sid
      String sid = fields[0];      
        String theDate = fields[4];
        
        //third field of input is assigned to theDate
        if (hr.contains(fields[3])) {
     
        if (Dates.contains(theDate))
        	try {
        		if(sid.startsWith("-"))
        			sid =" "+sid;//to avoid sid starting with "-" changing to #NAME in excel
        		String CompKey = theDate+"::"+sid;
        		
        		//Mapper writes theDate+"::"+sid as key and 1 as the value
				con.write(new Text(CompKey), new IntWritable(1));
				
			} catch (NullPointerException e) {
				e.printStackTrace();
			}     
			}
    }
      
    }
  }


