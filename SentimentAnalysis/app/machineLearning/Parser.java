package machineLearning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Class to implement parsing of data
 *
 * @author Ekal.Golas
 *
 */
public class Parser {
	/**
	 * Parses a folder for features
	 *
	 * @param location
	 *            Location of the folder
	 * @return List of lists of features, each list a document
	 * @throws IOException
	 */
	public static ArrayList<ArrayList<String>> parseFolder(String location)
			throws IOException {
		// Initialize array list of features
		ArrayList<ArrayList<String>> features = new ArrayList<>();

		// Get all files in the folder
		File[] files = new File(location).listFiles();
		for (File file : files) {
			// Initialize list for features in this document
			ArrayList<String> document = new ArrayList<>();

			// Read each file
			BufferedReader reader = new BufferedReader(new FileReader(
					file.getAbsolutePath()));
			String line = null;

			// Get all features for each line of the file
			while ((line = reader.readLine()) != null) {
				// Split document into words by spaces
				String[] words = line.split(" ");

				// Do for each word found
				for (String string : words)
					// Add features
					document.add(string);
			}

			// Add the document to the features
			features.add(document);
			reader.close();
		}

		// Return the features
		return features;
	}
}