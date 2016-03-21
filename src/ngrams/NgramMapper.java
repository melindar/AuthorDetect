package ngrams;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramMapper extends Mapper<NullWritable, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		String ngramType = conf.get("ngramType");
		
		Parser p = new Parser(ngramType, value);
		p.setHeaderInfo();
		p.tokenizeNgrams();
		
		for(String word : p.tokens)
		{
			word = word +  "\t" + p.getValue();
			keyString.set(word);
			Text title = new Text(p.getTitle());
			context.write(keyString,title);
		}

		
	}
}