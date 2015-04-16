package Homework3;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * User-defined function to format the genre
 * 
 * @author Ekal.Golas
 *
 */
@Description(name = "Format_Genre", value = "_FUNC_(str, NetID) - Formats the genre", extended = "Example:\n"
		+ "  > SELECT FORMAT_GENRE(author_name) FROM movies m;")
public class FormatGenre extends UDF {
	/**
	 * Function to format the genre
	 * 
	 * @param s
	 *            Text to format
	 * @param NetId
	 *            Net ID to add
	 * @return Formatted text
	 */
	public Text evaluate(Text s, Text NetId) {
		Text to_value = new Text("");
		if (s != null) {
			try {
				// Get the Genre and split it
				String[] genres = s.toString().split("\\|");

				// Initialize an output variable
				StringBuilder stringBuilder = new StringBuilder();

				// Iterate over array
				int i = 0;
				for (; i < genres.length - 2; i++)
					stringBuilder.append(i + 1 + ") " + genres[i] + ", ");

				// Form the last 2 genres
				if (i == genres.length - 2)
					stringBuilder.append(i + 1 + ") " + genres[i++] + " & ");

				stringBuilder.append(i + 1 + ") " + genres[i] + " "
						+ NetId.toString() + " :hive");

				// Set the result
				to_value.set(stringBuilder.toString());
			} catch (Exception e) {
				// Should never happen
				to_value = new Text(s);
			}
		}

		return to_value;
	}
}