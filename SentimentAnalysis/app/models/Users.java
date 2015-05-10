package models;

import play.db.ebean.*;
import play.data.validation.Constraints.*;

import javax.persistence.*;

/**
 * Entity Users
 * 
 * 
 *
 */
@Entity
@Table(name = "Users")
public class Users extends Model {
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
	 * User weight
	 */
	@Required
	@MaxLength(20)
	@MinLength(1)
	public double user_weight;

	
	/**
	 * Finder for this class
	 */
	public static Finder<String, Users> find = new Finder<String, Users>(
			String.class, Users.class);

	/**
	 * Parameterized constructor
	 * 
	 * @param id
	 *            User Id
	 * @param weight
	 *            User weight
	 */
	public Users(int id, double weight) {
		this.user_id = id;
		this.user_weight = weight;
	}
}