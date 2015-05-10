package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import models.Movies;
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
		for (File file : files)
			try (BufferedReader br = new BufferedReader(new FileReader(
					file.getPath()))) {
				tweets.add(br.readLine());
			}

		util.Rating rate = new util.Rating();
		double twitterScore = rate.twitterRating(tweets);

		result = "The rating for movie " + form.get().movie + " is "
				+ twitterScore;

		// Render result to home page
		List<Users> users = Users.find.all();
		return ok(rating.render(result, form.get().movie, users));
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