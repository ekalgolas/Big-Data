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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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

