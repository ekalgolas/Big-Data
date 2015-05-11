package controllers;

import java.io.IOException;
import java.sql.SQLException;

import models.Movies;
import play.Logger;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import util.Rating;
import views.html.confirm;

/**
 * Controller to get feedback and print confirmation
 * 
 * @author Ekal.Golas
 *
 */
public class Confirm extends Controller {
	/**
	 * Feedback form
	 */
	private final static Form<Feedback> feedbackForm = new Form<Feedback>(
			Feedback.class);

	/**
	 * Class to represent feedback form
	 * 
	 * @author Ekal.Golas
	 *
	 */
	public static class Feedback {
		/**
		 * Rating given for the movie
		 */
		public int rating;

		public double twitterRating;

		/**
		 * Comment given with the rating
		 */
		public String comment;

		/**
		 * Movie name
		 */
		public String movie;

		public int user;
	}

	/**
	 * AJAX action to get feedback and render confirmation page
	 * 
	 * @return Confirmation page
	 * @throws IOException
	 * @throws SQLException
	 */
	public static Result index() throws IOException, SQLException {
		// Initialize variables
		Form<Feedback> form = feedbackForm.bindFromRequest();
		Logger.info("Getting feedback for movie " + form.get().movie + "....");
		double rating = form.get().rating;
		int user = form.get().user;
		double twitter = form.get().twitterRating;
		int movie = Movies.find.where()
				.contains("movie_name", form.get().movie).findUnique().movie_id;

		Rating rate = new Rating();
		rate.insertRatingsRecord(user, movie, rating, twitter);
		rate.updateUserWeight(user);
		rate.updateMovieRating(movie);

		// Render result to home page
		return ok(confirm.render());
	}
}