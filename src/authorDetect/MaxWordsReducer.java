package authorDetect;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxWordsReducer extends Reducer<Text,Text,Text,DoubleWritable> 
{  

	DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		int max = 0;
		//Find the max value first
		for (Text val : values) 
		{
			String[] halfLine = val.toString().split("=");
			//String word = halfLine[0];
			int count = Integer.parseInt( halfLine[1]);
			
			if(count > max)
				max = count;
		}
		
		for(Text val : values)
		{
			String[] halfLine = val.toString().split("=");
			String word = halfLine[0];
			int count = Integer.parseInt( halfLine[1]);
			Text keystring = new Text(word + "_" + key);
			double fraction = (double)count / (double)max;
			result.set(fraction);
			context.write(keystring, result);
		}
		
	}

}
