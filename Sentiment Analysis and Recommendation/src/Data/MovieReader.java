/**
 * Reads movie URL and gets its details
 */
package Data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author Ekal.Golas
 *
 */
public class MovieReader {
	/**
	 * Main method
	 * 
	 * @param args
	 *            Command-line arguments
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		// Read the file into a hash set
		HashSet<String> movieIds = new HashSet<>();
		for (String arg : args)
			try (BufferedReader br = new BufferedReader(new FileReader(arg))) {
				String line = br.readLine();
				while (line != null) {
					movieIds.add(line.split("/")[4]);
					line = br.readLine();
				}
			}

		try (BufferedWriter br = new BufferedWriter(new FileWriter(
				"D:\\movies.txt"))) {
			for (Object ID : movieIds.toArray()) {
				InputStream input = new URL("http://www.omdbapi.com/?i="
						+ URLEncoder.encode(ID.toString(), "UTF-8"))
						.openStream();
				Map<String, String> map = new Gson().fromJson(
						new InputStreamReader(input, "UTF-8"),
						new TypeToken<Map<String, String>>() {
						}.getType());

				String title = map.get("Title");
				String genre = map.get("Genre");
				System.out.print(title + "-" + genre + "\n");
				br.write(title + "-" + genre + "\n");
			}
		}
	}
}