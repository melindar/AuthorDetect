package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TFIDFDriver extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new TFIDFDriver(), args);	
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		// Count the number of Authors

		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(1);
		job1.setJarByClass(TFIDFDriver.class);
		job1.setMapperClass(CountAuthorsMapper.class);
		job1.setReducerClass(CountAuthorsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "1"));
		job1.waitForCompletion(true);

		long result = job1.getCounters().findCounter("Result","Result").getValue();
		int numAuthors = ((int)result) / 1000;
		String numAuthorsString = "" + numAuthors;

		// Get the frequency of words by author

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(TFIDFDriver.class);
		job2.setMapperClass(FrequencyMapper.class);
		job2.setReducerClass(FrequencyReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "2"));
		job2.waitForCompletion(true);
		
		
		// Max number of words per author
		
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(TFIDFDriver.class);
		job3.setMapperClass(MaxWordsMapper.class);
		job3.setReducerClass(MaxWordsReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[1] + "2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "3"));
		job3.waitForCompletion(true);
		
		// Calculate TF-IDF
		
		Configuration conf4 = new Configuration();
		conf4.set("NumAuthors", numAuthorsString);
		Job job4 = Job.getInstance(conf4);
		job4.setJarByClass(TFIDFDriver.class);
		job4.setMapperClass(TFIDFMapper.class);
		job4.setReducerClass(TFIDFReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path(args[1] + "3"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "4"));
		job4.waitForCompletion(true);
		
		return 0;
	}
}
