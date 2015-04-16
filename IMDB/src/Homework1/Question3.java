package Homework1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class to find the movies with specified genre
 *
 * @author Ekal.Golas
 *
 */
public class Question3 {
	/**
	 * Mapper class
	 *
	 * @author Ekal.Golas
	 *
	 */
	public static class Map
			extends
				Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * Type of value
		 */
		private final static Text movie = new Text();

		/**
		 * Key as integer 1
		 */
		private final IntWritable one = new IntWritable(1);

		/**
		 * Genre passed from configuration
		 */
		private static String genre;

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

			if (row[2].toLowerCase().contains(genre.toLowerCase())) {
				// set movie
				movie.set(row[1]);

				// create key value pair
				context.write(one, movie);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
		 * .Mapper.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Get genre
			Configuration conf = context.getConfiguration();
			genre = conf.get("genre");
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
	 * Main Function
	 *
	 * @param args
	 *            Command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: Question3 <genre> <in> <out>");
			System.exit(2);
		}

		// Set genre
		conf.set("genre", otherArgs[0]);

		// create a job
		Job job = Job.getInstance(conf, "Question3");
		job.setJarByClass(Question3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(IntWritable.class);

		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}