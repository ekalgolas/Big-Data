package mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for 1 digit region key
 *
 * @author Ekal.Golas
 */
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * Type of value
	 */
	private final IntWritable	one			= new IntWritable(1);

	/**
	 * Type of value
	 */
	private final Text			outputKey	= new Text();

	/**
	 * Variable to store length of the key
	 */
	int							keyLength;

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(final Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		final Configuration conf = context.getConfiguration();

		// Retrieve key length set in main method
		keyLength = Integer.parseInt(conf.get("keyLength"));
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
		// Split the row to read different columns
		final String[] row = value.toString().split(",");

		// Ignore empty rows
		if ((row == null) || (row.length <= 5)) {
			return;
		}

		// Ignore null values for regions
		if ((row[4] == null) || (row[4].length() == 0)) {
			return;
		} else if ((row[5] == null) || (row[5].length() == 0)) {
			return;
		}

		// Get the first digit in the region and type to form the key
		if (row.length == 8) {
			// Else, set key as region and type
			outputKey.set(row[4].substring(0, keyLength) + "," + row[5].substring(0, keyLength) + "," + row[7]);
		} else if ((row[4].charAt(0) != 'E') && (row[4].charAt(0) != 'e')) {
			// If type is null, set as NA
			outputKey.set(row[4].substring(0, keyLength) + "," + row[5].substring(0, keyLength) + ",NA");
		} else {
			// Else, ignore header line
			return;
		}

		// Write the pair <key, 1>
		context.write(outputKey, one);
	}
}