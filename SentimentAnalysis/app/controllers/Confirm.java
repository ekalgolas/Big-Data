package controllers;

import java.io.IOException;

import play.Logger;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
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

		/**
		 * Comment given with the rating
		 */
		public String comment;

		/**
		 * Movie name
		 */
		public String movie;
	}

	/**
	 * AJAX action to get feedback and render confirmation page
	 * 
	 * @return Confirmation page
	 * @throws IOException
	 */
	public static Result index() throws IOException {
		// Initialize variables
		Form<Feedback> form = feedbackForm.bindFromRequest();
		Logger.info("Getting feedback for movie " + form.get().movie + "....");
		double rating = form.get().rating;
		String comment = form.get().comment;

		// Render result to home page
		return ok(confirm.render());
	}
}