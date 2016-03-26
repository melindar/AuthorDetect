package authorDetect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tfidf.MaxWordsMapper;
import tfidf.MaxWordsReducer;


public class AuthorDetectDriver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new AuthorDetectDriver(), args);
	}
	
	public int run(String[] args) throws Exception 
	{
		// Get the term frequency for the unknown file
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(1);
		job1.setJarByClass(AuthorDetectDriver.class);
		job1.setMapperClass(FrequencyMapper.class);
		job1.setReducerClass(FrequencyReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "5"));
		job1.waitForCompletion(true);
		
		
		// Calculate the max number of words for the unknown author
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(AuthorDetectDriver.class);
		job2.setMapperClass(MaxWordsMapper.class);
		job2.setReducerClass(MaxWordsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1] + "5"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "6"));
		job2.waitForCompletion(true);
		
		// Combine offline and command line tfidf

		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(AuthorDetectDriver.class);
		job3.setMapperClass(GroupingMapper.class);
		job3.setReducerClass(GroupingReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job3, new Path(args[1] + "4"), TextInputFormat.class, GroupingMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[1] + "6"), TextInputFormat.class, GroupingMapper.class);
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "7"));
		job3.waitForCompletion(true);
		
		return 0;
	}
}
