package ngrams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Ngram 
{

	public static void main(String[] args) throws Exception 
	{
		// Unigrams by date
		//int numReduceTasks = 20;
		
		/*Configuration conf1 = new Configuration();
		conf1.set("ngramType","unigramDate");
		Job job1 = Job.getInstance(conf1, "unigramDate");
		//job1.setNumReduceTasks(numReduceTasks);
		job1.setJarByClass(Ngram.class);
		job1.setMapperClass(NgramMapper.class);
		job1.setCombinerClass(NgramReducer.class);
		job1.setReducerClass(NgramReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + 1));
		//job1.waitForCompletion(true);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);*/
		
		// Unigrams by author
		
		Configuration conf2 = new Configuration();
		conf2.set("ngramType","unigramAuthor");
		Job job2 = Job.getInstance(conf2, "unigramAuthor");
		//job2.setNumReduceTasks(numReduceTasks);
		job2.setJarByClass(Ngram.class);
		job2.setMapperClass(NgramMapper.class);
		job2.setCombinerClass(NgramReducer.class);
		job2.setReducerClass(NgramReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + 2));
		job2.waitForCompletion(true);
		
		// Bigrams by date
		
		/*Configuration conf3 = new Configuration();
		conf3.set("ngramType","bigramDate");
		Job job3 = Job.getInstance(conf3, "bigramDate");
		//job3.setNumReduceTasks(numReduceTasks);
		job3.setJarByClass(Ngram.class);
		job3.setMapperClass(NgramMapper.class);
		job3.setCombinerClass(NgramReducer.class);
		job3.setReducerClass(NgramReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + 3));
		job3.waitForCompletion(true);
		
		// Bigrams by author
		
		Configuration conf4 = new Configuration();
		conf4.set("ngramType","bigramAuthor");
		Job job4 = Job.getInstance(conf4, "bigramAuthor");
		//job4.setNumReduceTasks(numReduceTasks);
		job4.setJarByClass(Ngram.class);
		job4.setMapperClass(NgramMapper.class);
		job4.setCombinerClass(NgramReducer.class);
		job4.setReducerClass(NgramReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + 4));
		System.exit(job4.waitForCompletion(true) ? 0 : 1);*/
	}
}
