package authorDetect;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxWordsReducer extends Reducer<Text,Text,Text,Text> 
{  

	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		ArrayList<String> lines = new ArrayList<String>();
		int max = 0;
		//Find the max value first
		for (Text val : values) 
		{
			String line = val.toString();
			lines.add(line);
			String[] halfLine = line.split("=");
			//String word = halfLine[0];
			int count = Integer.parseInt( halfLine[1]);
			
			if(count > max)
				max = count;
		}
		
		for(String line : lines)
		{
			String[] halfLine = line.split("=");
			String word = halfLine[0];
			int count = Integer.parseInt( halfLine[1]);
			Text keystring = new Text(word);
			double fraction = (double)count / (double)max;
			result.set("@@@" + fraction);
			context.write(keystring, result);
		}
		
	}

}
