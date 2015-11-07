import mapper.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import reducer.Combiner;
import reducer.Reduce;

/**
 * Driver program for map reduce that finds the number the crimes by type per region
 *
 * @author Ekal.Golas
 */
public class Crime {
	/**
	 * Main method
	 *
	 * @param args
	 *            Command line arguments
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: Crime <Input> <Output> <key-length>");
			System.exit(2);
		}
		else if ((Integer.parseInt(otherArgs[2]) < 1) || (Integer.parseInt(otherArgs[2]) > 6)) {
			System.err.println("Key length must be between 1 to 6");
			System.exit(3);
		}

		// Setting global data variables for hadoop
		conf.set("keyLength", otherArgs[2]);

		// create a job with
		final Job job = Job.getInstance(conf);
		job.setJobName("Crime reports");
		job.setJarByClass(Crime.class);

		// Set map reduce classes
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}