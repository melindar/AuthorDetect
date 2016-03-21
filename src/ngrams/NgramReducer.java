package ngrams;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NgramReducer extends Reducer<Text,Text,Text,Text> 
{
	  
  //private Text result = new Text();

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
  {
	  HashSet<String> unique = new HashSet<String>();

	  int matchCount = 0;
	  for(Text val : values) 
	  {
		  matchCount++;
		  unique.add(val.toString());
	  }
	  
	  int volCount = unique.size();
	  String prelimResult = matchCount + "\t" + volCount + "\n";
	  Text result = new Text(prelimResult);
	  context.write(key, result);
  }
}
