package authorDetect;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		Parser p = new Parser(value);
		p.tokenizeUnigrams();
		
		for(String word : p.tokens)
		{
			keyString.set(p.getAuthor());
			Text val = new Text(word);
			context.write(keyString,val);
		}

		
	}
}