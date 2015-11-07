package reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class
 *
 * @author Ekal.Golas
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	/**
	 * Type of output value
	 */
	private final IntWritable	result	= new IntWritable();

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
		// Set the result
		int sum = 0;
		for (final IntWritable intWritable : values) {
			sum += intWritable.get();
		}

		result.set(sum);

		// Set the key
		final String[] strings = key.toString().split(",");
		final String string = "Region (" + getPaddedString(strings[0]) + ", " + getPaddedString(strings[1]) + ") reported crimes of type " + strings[2] + " - ";

		// Create a pair <keyword, string>
		context.write(new Text(string), result);
	}

	/**
	 * Pads a string with character 'x'
	 *
	 * @param string
	 *            String to pad
	 * @return String of length x
	 */
	private String getPaddedString(final String string) {
		final String pad = "xxxxxx";
		return string + pad.substring(string.length());
	}
}