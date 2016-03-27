package authorDetect;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ADGroupingMapper extends Mapper<Object, Text, Text, Text> 
{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		String[] halfLine = line.split(":");
		Text keystring = new Text(halfLine[0].trim());
		Text val = new Text(halfLine[1].trim());
		context.write(keystring, val);
	}
}