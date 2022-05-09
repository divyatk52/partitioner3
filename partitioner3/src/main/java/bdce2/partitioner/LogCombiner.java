package bdce2.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogCombiner extends Reducer<Text, IntWritable,Text,IntWritable >{
	public void reduce(Text key, Iterable<IntWritable> values,
            Context context
            ) throws IOException, InterruptedException {
int sum = 0;

// initially sum is set as 0, before we start
// counting the occurrences	

for (IntWritable val : values) {
sum += val.get();

// for each occurrence we are adding the value (which is 1)
// so, in other words we are adding 1 for each occurrence 
	
}

// putting the value of result as the number of occurrences found

context.write(key, new IntWritable(sum));

// writing the sum

}


}
