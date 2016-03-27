package tfidf;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxWordsMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		String letters = line.substring(0,getLetterIndex(line));
		String numbers = line.substring(getLetterIndex(line));
		
		System.out.println("\n\n\n\n**************************\n" + line + "\n**************************\n\n\n\n");
		
		String[] halfLine = letters.split("_");
		String word = halfLine[0].trim();
		String author = halfLine[1].trim();
		
		int count = Integer.parseInt(numbers);
		String wordVal = word + "=" + count;
		Text val = new Text(wordVal);
		
		keyString.set(author);	
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