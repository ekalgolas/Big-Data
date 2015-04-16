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
 * Class to list all male user id whose age is less or equal to 7
 *
 * @author Ekal.Golas
 *
 */
public class Question1 {
	/**
	 * Mapper class
	 *
	 * @author Ekal.Golas
	 *
	 */
	public static class Map extends
	Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * Type of value
		 */
		private final static IntWritable age = new IntWritable(1);

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

			// If age is less than or equal to 7 and the user is male, map that
			// user
			if (Integer.parseInt(row[2]) <= 7 && row[1].equalsIgnoreCase("m")) {
				// set user as each input keyword
				user.set(row[0]);

				// Set its age as value
				age.set(Integer.parseInt(row[2]));

				// create a pair <keyword, 1>
				context.write(age, user);
			}
		}
	}

	/**
	 * Reducer class
	 *
	 * @author Ekal.Golas
	 *
	 */
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
		/**
		 * Type of output value
		 */
		private final Text result = new Text();

		/*
		 * (non-Javadoc)
		 *
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// Set the result
			StringBuilder stringBuilder = new StringBuilder();
			for (Text text : values)
				stringBuilder.append("\r\n" + text.toString());

			// create a pair <keyword, string>
			result.set(stringBuilder.toString());
			context.write(new Text(), result);
		}
	}

	/**
	 * Main method
	 *
	 * @param args
	 *            Command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// create a job with
		Job job = Job.getInstance();
		job.setJobName("Question 1");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(IntWritable.class);

		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}