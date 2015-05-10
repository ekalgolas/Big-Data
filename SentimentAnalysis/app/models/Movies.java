package models;

import play.db.ebean.*;
import play.data.validation.Constraints.*;

import javax.persistence.*;

/**
 * Entity Movies
 * 
 *
 */
@Entity
@Table(name = "Movies")
public class Movies extends Model {
	/**
	 * Default serial version ID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Movie Id
	 */
	@Required
	@Id
	public int movie_id;

	/**
	 * Movie Name
	 */
	@Required
	@MaxLength(30)
	public String movie_name;

	/**
	 * Movie Rating
	 */
	public double rating;

	/**
	 * Finder for this class
	 */
	public static Finder<String, Movies> find = new Finder<String, Movies>(
			String.class, Movies.class);

	/**
	 * Parameterized constructor
	 * 
	 * @param mid
	 *            Movie Id
	 * @param name
	 *            Movie name
	 * @param rating
	 *            Rating given to the movie
	 */
	public Movies(int mid, String name, double rating) {
		this.movie_id = mid;
		this.movie_name = name;
		this.rating = rating;
	}
}