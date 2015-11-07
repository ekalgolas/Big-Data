package reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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

		// Create a pair <keyword, string>
		context.write(key, result);
	}
}