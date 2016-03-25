package tfidf;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		int index = getLetterIndex(line);
		String word_author= line.substring(0,index).trim();
		String numbers = line.substring(index).trim();
		
		
		String[] halfLine = word_author.split("_");
		String word = halfLine[0].trim();
		String author = halfLine[1].trim();
		
		Text val = new Text(author + "_" + numbers);
		
		keyString.set(word);	
		context.write(keyString, val);	
	}
	
	private int getLetterIndex(String s)
	{
		int i = s.length();
		while(i > 0 && !Character.isWhitespace(s.charAt(i - 1)))
		{
			i --;
		}
		return i;
	}
}