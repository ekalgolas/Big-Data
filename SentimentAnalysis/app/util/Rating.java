package util;

import java.util.*;
import java.sql.*;

import play.Logger;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;

import machineLearning.LogisticRegression;
import models.Movies;
import models.Ratings;

public class Rating {
	/**
	 * Logistic regression classifier
	 */
	public static LogisticRegression logisticRegression = new LogisticRegression();

	DB dbinstance = new DB();

	public double twitterRating(List<String> tweets) {
		TweetScore tweetscore = new TweetScore();
		for (String tweet : tweets) {
			tweetscore.add(getScore(tweet));
			Logger.info(getScore(tweet) + "");
		}

		return tweetscore.AverageRating() * 10;
	}

	public double getScore(String tweet) {
		double tweet_score = 0.0;
		String[] words = tweet.split("\\s+");
		for (String word : words) {
			if (logisticRegression.weights.containsKey(word))
				tweet_score += logisticRegression.weights.get(word);
		}

		return tweet_score;
	}

	public void insertRatingsRecord(int uid, int mid, double userrating,
			double twitterrating) {
		double similarity = calculateSimilarity(userrating, twitterrating);
		Ratings rating = new Ratings(uid, mid, userrating, similarity);

		Ebean.save(rating);
		Logger.info("Movie rating updated for movie id " + mid + " by user "
				+ uid + "....");
	}

	public double calculateSimilarity(double user_rating, double twitter_rating) {
		double similarity = 0.0;
		if (twitter_rating >= user_rating)
			similarity = user_rating / twitter_rating;
		else
			similarity = 1 - (twitter_rating / user_rating);
		System.out.println("Simialrity = " + similarity);
		return similarity;
	}

	public void updateUserWeight(int uid) throws SQLException {
		List<Ratings> ratings = Ratings.find.where().eq("user_id", uid)
				.findList();

		double average = 0.0;
		int count = 0;
		for (Ratings r : ratings) {
			average = average + r.similarity;
			count++;
		}

		average = average / count;
		Logger.info(average + "");
		models.Users user = (models.Users) Ebean.find(models.Users.class, uid);
		user.user_weight = average;
		Ebean.save(user);

		Logger.info("User weight updated for user " + uid + " as " + average
				+ "....");
	}

	public void updateMovieRating(int mid) throws SQLException {
		String sql = "select user_weight, rating from ratings join users on ratings.user_id = users.user_id and ratings.movie_id="
				+ mid;
		List<SqlRow> results = Ebean.createSqlQuery(sql).findList();
		double weightedsum = 0.0;
		double weights = 0.0;
		double weightedavg = 0.0;
		for (SqlRow row : results) {
			double weight = row.getDouble("user_weight");
			weights = weights + weight;
			double ratings = row.getDouble("rating");
			weightedsum = weightedsum + (weight * ratings);
		}

		if (weights != 0)
			weightedavg = weightedsum / weights;

		Movies movie = (Movies) Ebean.find(Movies.class, mid);
		movie.rating = weightedavg;
		Ebean.save(movie);
		Logger.info("Movie " + mid + " updated!!....");
	}

	public double getMovieRating(int mid) throws SQLException {
		String sql = "select * from movies where movie_id = " + mid;
		ResultSet rs = dbinstance.executeQuery(sql);
		double result = 0.0;
		while (rs.next()) {
			result = rs.getDouble(3);
		}
		return result;
	}
}
