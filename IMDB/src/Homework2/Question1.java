package Homework2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class to find top 5 average movies rated by female users
 * 
 * @author Ekal.Golas
 *
 */
public class Question1 {
	/**
	 * Ratings mapper class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class RatingsMap
			extends
				Mapper<LongWritable, Text, LongWritable, Text> {
		/**
		 * Output key
		 */
		private LongWritable userID = new LongWritable();

		/**
		 * Output value
		 */
		private Text rating = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get column data
			String[] mydata = value.toString().split("::");

			// Set value
			rating = new Text("rat~" + value.toString().trim());

			// Set key
			userID.set(Integer.parseInt(mydata[0].trim()));

			// Write key value pair
			context.write(userID, rating);
		}
	}

	/**
	 * Users mapper class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class UsersMap
			extends
				Mapper<LongWritable, Text, LongWritable, Text> {
		/**
		 * Output key
		 */
		private LongWritable userID = new LongWritable();

		/**
		 * Output value
		 */
		private Text user = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get column data
			String[] mydata = value.toString().split("::");

			// Set value
			user = new Text("usr~" + value.toString());

			// Set key
			userID.set(Integer.parseInt(mydata[0]));

			// Write key value pair if user is female
			if (mydata[1].equalsIgnoreCase("f"))
				context.write(userID, user);
		}
	}

	/**
	 * Movies mapper class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class MoviesMap
			extends
				Mapper<LongWritable, Text, LongWritable, Text> {
		/**
		 * Output key
		 */
		private LongWritable movieID = new LongWritable();

		/**
		 * Output value
		 */
		private Text movie = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get column data
			String[] mydata = value.toString().split("::");

			// Set value
			movie = new Text("mov~" + value.toString());

			// Set key
			movieID.set(Integer.parseInt(mydata[0]));

			// Write key value pair
			context.write(movieID, movie);
		}
	}

	/**
	 * Intermediate reducer class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class IntermediateJoin
			extends
				Reducer<LongWritable, Text, Text, Text> {
		/**
		 * Map the counts
		 */
		private HashMap<String, Integer> countRatings = new HashMap<>();

		/**
		 * Map the sum
		 */
		private HashMap<String, Integer> sumRatings = new HashMap<>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// Initialize lists
			ArrayList<String> ratings = new ArrayList<>();
			ArrayList<String> users = new ArrayList<>();

			// Split data into different lists
			for (Text val : values)
				if (val.toString().contains("rat~"))
					ratings.add(val.toString().substring(4));
				else
					users.add(val.toString().substring(4));

			// Perform join
			for (String user : users) {
				for (String rating : ratings) {
					String[] r = rating.split("::");

					// Update sum and count maps for each join result
					if (r[0].equals(user.split("::")[0])) {
						if (sumRatings.containsKey(r[1])) {
							sumRatings.put(
									r[1],
									sumRatings.get(r[1])
											+ Integer.parseInt(r[2]));
							countRatings.put(r[1], countRatings.get(r[1]) + 1);
						} else {
							sumRatings.put(r[1], Integer.parseInt(r[2]));
							countRatings.put(r[1], 1);
						}
					}
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
		 * .Reducer.Context)
		 */
		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int i = 1; i <= 5; i++) {
				double max = Double.MIN_VALUE;
				String key = "";

				// Compute max average
				for (String movie : sumRatings.keySet())
					if ((sumRatings.get(movie) / (double) countRatings
							.get(movie)) > max) {
						max = (sumRatings.get(movie) / (double) countRatings
								.get(movie));
						key = movie;
					}

				// Write to context
				context.write(new Text(""),
						new Text(key + "::" + String.valueOf(max)));

				// Remove written values from hashmap
				sumRatings.remove(key);
				countRatings.remove(key);
			}
		}
	}

	/**
	 * Reducer class
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// Initialize lists
			ArrayList<String> ratings = new ArrayList<>();
			ArrayList<String> movies = new ArrayList<>();

			// Split data into different lists
			for (Text val : values)
				if (val.toString().contains("rat~"))
					ratings.add(val.toString().substring(4));
				else
					movies.add(val.toString().substring(4));

			// Perform join and write the result
			for (String movie : movies) {
				for (String rating : ratings) {
					context.write(new Text(movie.split("::")[1]), new Text(
							rating.split("::")[1]));
				}
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
		if (otherArgs.length != 5) {
			System.err
					.println("Usage: HomeWork2.Question1 <RatingsInput> <UsersInput> <MoviesInput> <IntermediateOutput> <finalOutput>");
			System.exit(2);
		}

		// create a job
		Job job = Job.getInstance(conf, "Question1");
		job.setJarByClass(Homework2.Question1.class);

		job.setReducerClass(IntermediateJoin.class);

		// Add input paths
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, RatingsMap.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, UsersMap.class);

		// Set output key type
		job.setOutputKeyClass(LongWritable.class);

		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		if (job.waitForCompletion(true)) {
			// create a job after intermediate join is done
			Job job2 = Job.getInstance(conf, "Question1");
			job2.setJarByClass(Homework2.Question1.class);

			job2.setReducerClass(Reduce.class);

			// Add input paths
			MultipleInputs.addInputPath(job2, new Path(otherArgs[3]),
					TextInputFormat.class, RatingsMap.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[2]),
					TextInputFormat.class, MoviesMap.class);

			// Set output key type
			job2.setOutputKeyClass(LongWritable.class);

			// set output value type
			job2.setOutputValueClass(Text.class);

			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));

			job2.waitForCompletion(true);
		}
	}
}