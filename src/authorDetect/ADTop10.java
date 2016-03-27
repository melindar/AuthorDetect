package authorDetect;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ADTop10 
{
	/********************************** START Mapper Inner Class **********************************/
	public static class ADTop10Mapper extends Mapper<Object, Text, DoubleWritable, Text> 
	{
		private TreeMap<DoubleWritable,Text> topAuthors = new TreeMap<DoubleWritable, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString().trim();
			int index = getLetterIndex(line);
			String author= line.substring(0,index).trim();
			String numbers = line.substring(index).trim();
			Double num = Double.parseDouble(numbers);
			
			Text t = new Text(author);
			DoubleWritable d = new DoubleWritable(num);
			
			topAuthors.put(d,t);
			if(topAuthors.size() > 10)
			{
				topAuthors.remove(topAuthors.firstKey());
			}
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Map.Entry<DoubleWritable,Text> entry : topAuthors.entrySet())
			{
				Text author = entry.getValue();
				DoubleWritable value = entry.getKey();
				context.write(value, author);
			}
		}
	}
	/********************************** END Mapper Inner Class **********************************/
	
	/********************************** START Reducer Inner Class **********************************/
	
	public static class ADTop10Reducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text> 
	{  
		private TreeMap<DoubleWritable, Text> topAuthors = new TreeMap<DoubleWritable, Text>();

		public void reduce(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			topAuthors.put(key,value);
			if(topAuthors.size() > 10)
			{
				topAuthors.remove(topAuthors.firstKey());
			}
			
			for(Map.Entry<DoubleWritable,Text> entry : topAuthors.entrySet())
			{
				Text author = entry.getValue();
				DoubleWritable val = entry.getKey();
				context.write(val, author);
			}
		}

	}
	
	
	
	/********************************** END Reducer Inner Class **********************************/

	public static int getLetterIndex(String s)
	{
		int i = s.length();
		while(i > 0 && !Character.isWhitespace(s.charAt(i - 1)))
		{
			i --;
		}
		return i;
	}
}
