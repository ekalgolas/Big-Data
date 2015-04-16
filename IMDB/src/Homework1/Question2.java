package Homework1;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class to find the count of female and males users in each age group
 *
 * @author Ekal.Golas
 *
 */
public class Question2 {
	/**
	 * Mapper class
	 *
	 * @author Ekal.Golas
	 *
	 */
	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		/**
		 * Type of value
		 */
		private final static IntWritable one = new IntWritable(1);

		/**
		 * Type of key
		 */
		private final Text user = new Text();

		/*
		 * (non-Javadoc)
		 *
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the row to read different columns
			String[] row = value.toString().split("::");

			// set age and gender as each input keyword
			user.set(row[2] + "\t" + row[1]);

			// create a pair <keyword, 1>
			context.write(user, one);
		}
	}

	/**
	 * Reducer class
	 *
	 * @author Ekal.Golas
	 *
	 */
	public static class Reduce extends
	Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * Type of output value
		 */
		private final IntWritable result = new IntWritable();

		/*
		 * (non-Javadoc)
		 *
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// Compute the sum
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();

			// create a pair <keyword, string>
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * Main function
	 * 
	 * @param args
	 *            Command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// create a job
		Job job = Job.getInstance();
		job.setJobName("Question 2");
		job.setJarByClass(Question2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner
		job.setCombinerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}