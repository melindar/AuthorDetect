package authorDetect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private Text keyString = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key,Text value, Context context) throws IOException, InterruptedException 
	{
		String[] halfLine = value.toString().split("<===>");
		ArrayList<String> tokens = tokenizeUnigramLine(halfLine[1]);
		
		for(String token : tokens)
		{
			String word_author = token + "_" + halfLine[0];
			keyString.set(word_author);
			//IntWritable one = new IntWritable();
			context.write(keyString,one);
		}
	}
	
	private ArrayList<String> tokenizeUnigramLine(String line)
	{
		ArrayList<String> tokens = new ArrayList<String>();
		StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) 
        {
        	String token = itr.nextToken().trim();
        	String word = removePunctuation(token);
        	if(word.length() > 0) //Make sure the string is not just whitespace
        	{
        		tokens.add(word);
        	}
        }
        
        return tokens;
	}
	
	private String removePunctuation(String s)
	{
		s = s.replaceAll("\\p{Punct}+", "");
		s = s.replaceAll("[^a-zA-Z0-9]", "");
		return s.toLowerCase();
	}
}