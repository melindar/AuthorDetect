package authorDetect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ADCosineSimMapper extends Mapper<Object, Text, Text, Text> 
{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String wholeLine = value.toString().trim();
		if(wholeLine.contains("@@@"))
		{
			int index = getwordIndex(wholeLine); // Find where the key and values should be split
			String word = wholeLine.substring(0,index).trim(); // The word is the key
			String line = wholeLine.substring(index).trim();  // The rest of the line has all the values
			HashMap<String, Double> compares = new HashMap<String, Double>();
			Double tf = 0.0;
			Double idf = 0.0;
			String[] tokens = line.split("\\$");
			for(int i = 0; i < tokens.length; i++)
			{
				String token = tokens[i].trim();
				if(token.length() < 1) //This is an empty line
					continue;
				else if(token.startsWith("@@@")) // This is the unknown tf value
					tf = Double.parseDouble(token.replaceAll("\\@{3}", ""));
				else if (token.endsWith("***"))  // This is the idf value
					idf = Double.parseDouble(token.replaceAll("\\*{3}", ""));
				else  // This is in the form author_tfidf and needs to be compared with the unknown
				{
					String[] author_tfidf = token.split("\\_");
					if(author_tfidf.length == 2)
					{
						String author = author_tfidf[0].trim();
						Double tfidf = Double.parseDouble(author_tfidf[1].trim());
						compares.put(author, tfidf);
					}
					
				}
			}

			// Calculate the tfidf of the unknown
			Double unknownTFIDF = tf * idf;
			
			// for each pair of known:unknown send to the reducer
			Iterator<Entry<String, Double>> it = compares.entrySet().iterator();
		    while (it.hasNext()) 
		    {
		    	Entry<String, Double> pair = it.next();
		    	Text keystring = new Text(pair.getKey());
				Text val = new Text(word + "_" + pair.getValue() + "&" + unknownTFIDF);
				context.write(keystring, val);
		    }
			
		}
	}
	
	private static int getwordIndex(String s)
	{
		int i = 0;
		while(i < s.length() && !Character.isWhitespace(s.charAt(i)))
		{
			i ++;
		}
		return i;
	}
}