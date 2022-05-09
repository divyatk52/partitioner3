package bdce2.partitioner;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*Counts the number of values associated with a key */

public class LogReducer extends Reducer<Text, IntWritable,Text,IntWritable > {
	public Map<String,Integer> map = new HashMap<String,Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {


		int count = 0;
		
		
		for (IntWritable value: values) {
			count += value.get();
			
		}
		//the key and values are put to a hashmap
		map.put(key.toString(),count);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{ 
		
		//Cleanup is called once at the end to finish off anything for reducer
	    //Here we will write our final output
		
	    Map<String,Integer> sortedByValue = sortByValue(map);

		int i = 0;
		for (Entry<String,Integer> entry : sortedByValue.entrySet()) {
		    if (i < 500) {
		       // Prints top 500 Songs played for that particular date
		        context.write(new Text(entry.getKey() ), new IntWritable(entry.getValue()));
		        i++;
		    }
		    else {
		       
		        break;
		    }
		}
	  }

	
	//To Sort values(count) in descending order using linkedlist of a hashmap to include duplicate values also.
	
	private <String,Integer extends Comparable<? super Integer>> Map<String,Integer> sortByValue(Map<String,Integer> map2) {
		// TODO Auto-generated method stub
		
			List<Map.Entry<String,Integer>> list =
		        new LinkedList<Map.Entry<String,Integer>>( map2.entrySet() );
			
		    Collections.sort( list, new Comparator<Map.Entry<String,Integer>>()
		    {
		        public int compare( Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2 )
		        {
		            return (o2.getValue()).compareTo( o1.getValue() );//to sort in desc order
		        }
		    } );
		    

		    Map<String,Integer> result = new LinkedHashMap<String,Integer>();
		    for (Map.Entry<String,Integer> entry : list)
		    {
		        result.put( entry.getKey(), entry.getValue() );
		    }
		    return result;
	}
}




