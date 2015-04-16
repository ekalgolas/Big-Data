package Homework3;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Class to provide a UDF to format the genre
 * 
 * @author Ekal.Golas
 *
 */
public class FORMAT_GENRE extends EvalFunc<String> {
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public String exec(Tuple input) {
		try {
			// Check for null
			if (input == null || input.size() == 0) {
				return null;
			}

			// Get the Genre and split it
			String genre = input.get(0).toString().trim();
			String[] genres = genre.split("\\|");

			// Initialize an output variable
			StringBuilder stringBuilder = new StringBuilder();

			// Iterate over array
			int i = 0;
			for (; i < genres.length - 2; i++)
				stringBuilder.append(i + 1 + ") " + genres[i] + ", ");

			// Form the last 2 genres
			if (i == genres.length - 2)
				stringBuilder.append(i + 1 + ") " + genres[i++] + " & ");

			stringBuilder.append(i + 1 + ") " + genres[i] + " ");

			// Return the result
			return stringBuilder.toString();
		} catch (ExecException ex) {
			System.out.println("Error: " + ex.toString());
		}

		return null;
	}
}