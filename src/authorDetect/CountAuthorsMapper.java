package authorDetect;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountAuthorsMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] halfLine = value.toString().split("<===>");
			
		keyString.set("authors"); 
		Text author = new Text(halfLine[0]); //First part is the author, second part is the line
		context.write(keyString, author);
		
	}
}
