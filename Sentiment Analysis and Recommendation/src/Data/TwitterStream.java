package Data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

/**
 * Class to implement twitter streaming
 * 
 * @author Ekal.Golas
 *
 */
public class TwitterStream {
	/**
	 * Twitter streaming context
	 */
	static JavaStreamingContext jssc;

	/**
	 * Starts live streaming
	 * 
	 * @param args
	 *            Movie name for which tweets are fetched
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		// Set streaming parameters
		SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		jssc = new JavaStreamingContext(sc, new Duration(1000));

		System.setProperty("twitter4j.oauth.consumerKey",
				"9IWEcdDBASj8347Zet7WZwMZ9");
		System.setProperty("twitter4j.oauth.consumerSecret",
				"sm9cLIqRVSvHHnvvjXmF6IY0HFgjvM3jBxaRqu9xpUjCidhZnQ");
		System.setProperty("twitter4j.oauth.accessToken",
				"2448140394-xDCrPgam5XlrZB7DA4W6QVoIx0n7zW5EDg6mcg5");
		System.setProperty("twitter4j.oauth.accessTokenSecret",
				"b77FPn6bxrTGGmkomXrz6jPZdmUL9HbC5EVSegOMXtwDz");

		// Set filters
		String[] filters = new String[5];
		final String movie = args[0];
		filters[0] = "9IWEcdDBASj8347Zet7WZwMZ9";
		filters[1] = "sm9cLIqRVSvHHnvvjXmF6IY0HFgjvM3jBxaRqu9xpUjCidhZnQ";
		filters[2] = "2448140394-xDCrPgam5XlrZB7DA4W6QVoIx0n7zW5EDg6mcg5";
		filters[3] = "b77FPn6bxrTGGmkomXrz6jPZdmUL9HbC5EVSegOMXtwDz";
		filters[4] = movie;
		JavaReceiverInputDStream<Status> receiverStream = TwitterUtils
				.createStream(jssc, filters);

		// Write each tweet to a file
		receiverStream.foreachRDD(new Function<JavaRDD<Status>, Void>() {
			@Override
			public Void call(JavaRDD<Status> arg0) throws Exception {
				arg0.foreach(new VoidFunction<Status>() {
					public void call(Status status) throws IOException {
						System.out.println(status.getText());

						// Get the most recent file
						String path = "D:\\Github\\Big-Data\\SentimentAnalysis\\dataset\\liveData\\";
						File dir = new File(path + ".");
						File[] files = dir.listFiles(new FilenameFilter() {
							@Override
							public boolean accept(File dir, String name) {
								return name.startsWith(movie + "_");
							}
						});

						int max = 0;
						for (File file : files) {
							int temp = Integer.parseInt(file.getName().split(
									"_")[1]);
							if (temp > max)
								max = temp;
						}

						// Write the tweets to a file
						max++;
						try (BufferedWriter br = new BufferedWriter(
								new FileWriter(path + movie + "_" + max))) {
							br.write(status.getText());
						}
					}
				});

				return null;
			}
		});

		// Start streaming
		jssc.start();
	}

	/**
	 * Stops live streaming
	 */
	public static void stop() {
		jssc.stop();
	}
}