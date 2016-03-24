package authorDetect;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountAuthorsReducer extends Reducer<Text,Text,Text,IntWritable> 
{
    private IntWritable result = new IntWritable(); 
    public float floatvalue;

	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	  {
		  HashSet<String> unique = new HashSet<String>();
		  for(Text val : values) 
		  {
			  unique.add(val.toString());
		  }
		  
		  floatvalue = unique.size();
	      result.set(unique.size());
	      context.write(key, result);
	  }
	  
	  public void cleanup(Context context) {

		    long result = (long) (floatvalue * 1000);
		    context.getCounter("Result","Result").increment(result); 

		}
	}
