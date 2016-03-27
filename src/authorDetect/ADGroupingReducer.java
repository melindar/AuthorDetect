package authorDetect;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ADGroupingReducer extends Reducer<Text,Text,Text,Text> 
{  

	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String result = "";
		for(Text value:values)
		{
			result += value.toString();
		}
		
		Text val = new Text(result);
		context.write(key, val);
	}

}
