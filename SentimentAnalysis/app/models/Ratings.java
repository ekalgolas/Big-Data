package models;

import play.db.ebean.*;
import play.data.validation.Constraints.*;

import javax.persistence.*;

/**
 * Entity Ratings
 * 
 *
 */
@Entity
@Table(name = "Ratings")
public class Ratings extends Model {
	/**
	 * Default serial version ID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * User Id
	 */
	@Required
	@Id
	public int user_id;

	/**
	 * Movie Id
	 */
	@Required
	@Id
	public int movie_id;

	/**
	 * Rating
	 */
	@Required
	public double rating;
	
	/**
	 * Similarity
	 */
	public double similarity;

	/**
	 * Finder for this class
	 */
	public static Finder<String, Ratings> find = new Finder<String, Ratings>(
			String.class, Ratings.class);

	/**
	 * Parameterized constructor
	 * 
	 * @uid
	 *            User Id
	 * @mid
	 *            Movie Id
	 * @rating
	 *            Rating given to mid by uid
	 * @similarity 
	 *				Similarity of rating with twitter rating
	 */
	public Ratings(int uid, int mid, double rating, double similar) {
		this.user_id = uid;
		this.movie_id = mid;
		this.rating = rating;
		this.similarity = similar;
	}
}