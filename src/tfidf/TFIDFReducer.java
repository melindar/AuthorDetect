package tfidf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text,Text,Text,Text> 
{  

	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		String numAuthorsString = conf.get("NumAuthors");
		int numAuthors = Integer.parseInt(numAuthorsString);
		ArrayList <String> authorVals = new ArrayList<String>();
		for(Text val : values)
		{
			authorVals.add(val.toString());
		}
		
		int numAuthorsUsedWord = authorVals.size();
		String author_tfidf = "";
		double idf = (double) (Math.log(numAuthors/numAuthorsUsedWord) / (double) Math.log(2));
		
		for(String s : authorVals)
		{
			String[] halfLine = s.split("_");
			String author = halfLine[0].trim();
			String numString = halfLine[1].trim();
			double tf = Double.parseDouble(numString);
			double tfidf = tf * idf;
			author_tfidf += author + "_" + tfidf + ",";
		}
		
		Text keyString = new Text(key + "_" + idf);
		result.set(author_tfidf);
		context.write(keyString,result);
		
	}
}
