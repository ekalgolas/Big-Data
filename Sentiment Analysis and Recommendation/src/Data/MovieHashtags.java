package Data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Sudhanshu.Saxena
 *
 */

public class MovieHashtags {

	private static String[] unwantedChars = {"\\s+", "'", ",", "#", ":", "!", "\\."};
	
	public static void main(String[] args) throws FileNotFoundException,
	IOException {
		String fileName = "dataset/movies.txt";
		String outputFile = "dataset/movieHashtags.txt";

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		BufferedWriter output = new BufferedWriter(new FileWriter(outputFile));
		String line = br.readLine();
		String movieName, hashtag;
		while (line != null) {
			movieName = line.split("-")[0];
			hashtag = removeUnwantedChars(movieName);
			System.out.println(hashtag);
			output.write("#"+hashtag+"\n");
			line = br.readLine();
		}
		br.close();
		output.close();
	}
	
	private static String removeUnwantedChars(String movie){
		movie = movie.trim();
		for(int i=0;i<unwantedChars.length;i++){
			movie = movie.replaceAll(unwantedChars[i], "");
		}
		return movie;
	}
}

