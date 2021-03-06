package controllers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import models.Movies;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;

/**
 * Controller to render the home page
 * 
 * @author Ekal.Golas
 *
 */
public class Application extends Controller {
	/**
	 * Renders the home page
	 * 
	 * @return Routes to index
	 * @throws IOException
	 */
	public static Result index() throws IOException {
		// Initialize variables
		String fileName = "dataset/movies.txt";
		List<String> movies = new ArrayList<>();

		// Read the movies into a list
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String line = br.readLine();
			while (line != null) {
				Movies movie = (Movies) Movies.find.where()
						.contains("movie_name", line.split("-")[0])
						.findUnique();
				movies.add(movie.movie_name + " : " + movie.rating);
				line = br.readLine();
			}
		}

		// Render result to home page
		return ok(index.render(movies));
	}
}