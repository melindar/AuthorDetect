package authorDetect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import authorDetect.ADTop10.ADTop10Mapper;
import authorDetect.ADTop10.ADTop10Reducer;


public class AuthorDetectDriver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new AuthorDetectDriver(), args);
	}
	
	public int run(String[] args) throws Exception 
	{/*
		// Get the term frequency for the unknown file
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(1);
		job1.setJarByClass(AuthorDetectDriver.class);
		job1.setMapperClass(ADFrequencyMapper.class);
		job1.setReducerClass(ADFrequencyReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("AD/" + args[1] + "5"));
		job1.waitForCompletion(true);
		
		
		// Calculate the max number of words for the unknown author
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(AuthorDetectDriver.class);
		job2.setMapperClass(ADMaxWordsMapper.class);
		job2.setReducerClass(ADMaxWordsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("AD/" + args[1] + "5"));
		FileOutputFormat.setOutputPath(job2, new Path("AD/" + args[1] + "6"));
		job2.waitForCompletion(true);
		
		// Combine offline and command line tfidf

		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(AuthorDetectDriver.class);
		job3.setMapperClass(ADGroupingMapper.class);
		job3.setReducerClass(ADGroupingReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job3, new Path("tfidf/" + args[1] + "4"), TextInputFormat.class, ADGroupingMapper.class);
		MultipleInputs.addInputPath(job3, new Path("AD/" + args[1] + "6"), TextInputFormat.class, ADGroupingMapper.class);
		FileOutputFormat.setOutputPath(job3, new Path("AD/" + args[1] + "7"));
		job3.waitForCompletion(true);

		// Calculate TF-IDF

		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4);
		job4.setJarByClass(AuthorDetectDriver.class);
		job4.setMapperClass(ADCosineSimMapper.class);
		job4.setReducerClass(ADCosineSimReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path("AD/" + args[1] + "7"));
		FileOutputFormat.setOutputPath(job4, new Path("AD/" + args[1] + "8"));
		job4.waitForCompletion(true);
		*/
		// Select Top 10

		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5);
		job5.setNumReduceTasks(1);
		job5.setJarByClass(AuthorDetectDriver.class);
		job5.setMapperClass(ADTop10Mapper.class);
		//job5.setCombinerClass(ADTop10Reducer.class);
		//job5.setReducerClass(ADTop10Reducer.class);
		job5.setOutputKeyClass(NullWritable.class);
		job5.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job5, new Path("AD/" + args[1] + "8"));
		FileOutputFormat.setOutputPath(job5, new Path("AD/" + args[1] + "9"));
		job5.waitForCompletion(true);
				
		
		return 0;
	}
}
