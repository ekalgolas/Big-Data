package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.Files;

import models.Users;
import play.Logger;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.*;

/**
 * Controller to display rating of a movie
 * 
 * @author Ekal.Golas
 *
 */
public class Rating extends Controller {
	/**
	 * Movie form
	 */
	private final static Form<Movie> movieForm = new Form<Movie>(Movie.class);

	/**
	 * AJAX action to find ratings and render feedback form
	 * 
	 * @return Renders ratings page
	 * @throws IOException
	 */
	public static Result index() throws IOException {
		// Initialize variables
		Form<Movie> form = movieForm.bindFromRequest();
		Logger.info("Finding movie rating for movie " + form.get().movie
				+ "....");
		String result = "";

		String path = "D:\\Github\\Big-Data\\SentimentAnalysis\\dataset\\liveData\\";
		File dir = new File(path + ".");
		File[] files = dir.listFiles();

		List<String> tweets = new ArrayList<>();
		ArrayList<ArrayList<String>> data = new ArrayList<>();
		for (File file : files)
			try (BufferedReader br = new BufferedReader(new FileReader(
					file.getPath()))) {
				String line = br.readLine();
				ArrayList<String> s = new ArrayList<>();
				for (String l : line.split(" "))
					s.add(l);

				data.add(s);
				tweets.add(line);
			}

		util.Rating rate = new util.Rating();
		double twitterScore = rate.twitterRating(tweets);

		if (!Double.isNaN(twitterScore))
			result = "The rating for movie " + form.get().movie + " is "
					+ twitterScore;
		else
			result = "No data available, please wait for the data from live stream";

		if (twitterScore > 2)
			util.Rating.logisticRegression.train(data, new ArrayList<>(), 0.1,
					50, 0.001);
		else
			util.Rating.logisticRegression.train(new ArrayList<>(), data, 0.1,
					50, 0.001);

		for (File file : files)
			Files.move(file,
					new File("dataset/processedData/" + file.getName()));

		// Render result to home page
		List<Users> users = Users.find.all();
		return ok(rating.render(result, form.get().movie.split(":")[0], users,
				twitterScore));
	}

	/**
	 * Class to represent movie form
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class Movie {
		/**
		 * Movie name
		 */
		public String movie;
	}
}