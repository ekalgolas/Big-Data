package Homework2;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2 {
	/**
	 * Mapper class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * Variable to store movie ID
		 */
		String mymovieid;

		/**
		 * Map to store users
		 */
		HashMap<String, String> userMap = new HashMap<>();

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
			super.setup(context);
			Configuration conf = context.getConfiguration();

			// Retrieve movie id set in main method
			mymovieid = conf.get("movieid");

			// Load files into the cache
			Path[] cacheFilesLocal = DistributedCache
					.getLocalCacheFiles(context.getConfiguration());
			for (Path eachPath : cacheFilesLocal)
				load(eachPath.toString());
		}

		/**
		 * Loads the cache file to a map
		 * 
		 * @param filePath
		 *            Path of the file
		 * @throws IOException
		 */
		private void load(String filePath) throws IOException {
			// Initialize variables
			BufferedReader brReader = null;
			String strLineRead = "";

			try {
				brReader = new BufferedReader(new FileReader(filePath));

				// Read each line, split and load to HashMap
				while ((strLineRead = brReader.readLine()) != null) {
					String users[] = strLineRead.split("::");
					userMap.put(users[0], strLineRead);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (brReader != null)
					brReader.close();
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split data
			String[] mydata = value.toString().split("::");

			// Perform join and then write
			if (mydata[1].equalsIgnoreCase(mymovieid)
					&& Integer.parseInt(mydata[2]) >= 4
					&& userMap.containsKey(mydata[0])) {
				// Get values
				String gender = userMap.get(mydata[0]).split("::")[1];
				String age = userMap.get(mydata[0]).split("::")[2];

				// Write the result
				context.write(new Text(mydata[0]),
						new Text(gender + "\t" + age));
			}
		}
	}

	/**
	 * Driver code
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
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: Homework2.Question2 <MovieInput> <UsersInput> <out> <anymovieid>");
			System.exit(2);
		}

		// setting global data variables for hadoop
		conf.set("movieid", otherArgs[3]);

		// Add users file to cache
		DistributedCache.addCacheFile(new URI(otherArgs[1]), conf);

		// create a job
		Job job = Job.getInstance(conf, "Question2");
		job.setJarByClass(Question2.class);
		job.setNumReduceTasks(0);

		// set mapper class
		job.setMapperClass(Map.class);

		// set the HDFS path of the input data
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}
}