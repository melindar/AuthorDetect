package authorDetect;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountAuthorsMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private Text keyString = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] halfLine = value.toString().split("<===>");
			
		keyString.set(halfLine[0]); //First part is the author, second part is the line
		context.write(keyString, one);
		
	}
}
