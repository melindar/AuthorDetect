package authorDetect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MapReduceDriver extends Configured implements Tool
{

	private static final String OUTPUT_PATH = "intermediate_output";
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new MapReduceDriver(), args);
		/*
		// Use this when needing to get numAuthors: 
		Configuration conf = context.getConfiguration();
		String numAuthorsString = conf.get("numAuthors");
		int numAuthors =  Integer.parseInt(numAuthorsString);
		
		// Count the number of Authors
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(1);
		job1.setJarByClass(MapReduceDriver.class);
		job1.setMapperClass(CountAuthorsMapper.class);
		job1.setReducerClass(CountAuthorsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + 1));
		job1.waitForCompletion(true);
		
		long result = job1.getCounters().findCounter("Result","Result").getValue();
	    int numAuthors = ((int)result) / 1000;
	    System.out.println("\n\n\n\n**************************\n" + numAuthors + "\n**************************\n\n\n\n");
		
	    // Get the frequency of words by author
	    
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(MapReduceDriver.class);
		job2.setMapperClass(FrequencyMapper.class);
		job2.setCombinerClass(FrequencyReducer.class);
		job2.setReducerClass(FrequencyReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + 2));
		job2.waitForCompletion(true);
		
		// *****Here is part of the last assignment, needs updating or commenting *****
		
		Configuration conf3 = new Configuration();
		//conf3.set("ngramType","bigramDate");
		Job job3 = Job.getInstance(conf3, "bigramDate");
		//job3.setNumReduceTasks(numReduceTasks);
		job3.setJarByClass(Ngram.class);
		job3.setMapperClass(NgramMapper.class);
		job3.setCombinerClass(NgramReducer.class);
		job3.setReducerClass(NgramReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		//job3.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + 3));
		job3.waitForCompletion(true);
		
		// Bigrams by author
		
		Configuration conf4 = new Configuration();
		//conf4.set("ngramType","bigramAuthor");
		Job job4 = Job.getInstance(conf4, "bigramAuthor");
		//job4.setNumReduceTasks(numReduceTasks);
		job4.setJarByClass(Ngram.class);
		job4.setMapperClass(NgramMapper.class);
		job4.setCombinerClass(NgramReducer.class);
		job4.setReducerClass(NgramReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		//job4.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + 4));
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		*/
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception 
	{
		// Count the number of Authors

		/*Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(1);
		job1.setJarByClass(MapReduceDriver.class);
		job1.setMapperClass(CountAuthorsMapper.class);
		job1.setReducerClass(CountAuthorsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + 1));
		job1.waitForCompletion(true);

		long result = job1.getCounters().findCounter("Result","Result").getValue();
		int numAuthors = ((int)result) / 1000;
		System.out.println("\n\n\n\n**************************\n" + numAuthors + "\n**************************\n\n\n\n");
*/
		// Get the frequency of words by author

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(MapReduceDriver.class);
		job2.setMapperClass(FrequencyMapper.class);
		job2.setReducerClass(FrequencyReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		Path inputPath = new Path(args[1] + 2);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, inputPath);
		//TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH));
		inputPath.getFileSystem(conf2).delete(inputPath);
		job2.waitForCompletion(true);
		
		
		// Max number of words per author
		

		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(MapReduceDriver.class);
		job3.setMapperClass(MaxWordsMapper.class);
		job3.setReducerClass(MaxWordsReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		//Path inputPath = new Path(args[1] + 2);
		FileInputFormat.addInputPath(job3, inputPath);
		//TextInputFormat.addInputPath(job3, new Path(OUTPUT_PATH));
		//TextOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + 5));
		job3.waitForCompletion(true);
		
		return 0;
	}
}
