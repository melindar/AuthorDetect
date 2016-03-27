package authorDetect;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ADCosineSimReducer extends Reducer<Text,Text,Text,DoubleWritable> 
{  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String result = "";
		double numerator = 0.0;
		double denominator = 0.0;
		double aSquareds = 0.0;
		double bSquareds = 0.0;
		
		for(Text value:values)
		{
			result = value.toString();
			String[] word_vals = result.split("\\_");
			String[] vals = word_vals[1].split("\\&");
			
			Double a = Double.parseDouble(vals[0]);
			Double b = Double.parseDouble(vals[1]);
			
			numerator += (a * b);
			aSquareds += (a * a);
			bSquareds += (b * b);
		}
		
		denominator = (Math.sqrt(aSquareds)) * (Math.sqrt(bSquareds));
		double cosSim = numerator / denominator;
		
		DoubleWritable val = new DoubleWritable(cosSim);
		context.write(key, val);
	}

}
