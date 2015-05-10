package util;

import java.util.*;
import java.sql.*;

import play.Logger;

import com.avaje.ebean.Ebean;

import machineLearning.LogisticRegression;
import models.Ratings;

public class Rating {
	/**
	 * Logistic regression classifier
	 */
	public static LogisticRegression logisticRegression = new LogisticRegression();

	DB dbinstance = new DB();

	public double twitterRating(List<String> tweets) {
		TweetScore tweetscore = new TweetScore();
		for (String tweet : tweets)
			tweetscore.add(getScore(tweet));

		return tweetscore.AverageRating();
	}

	public double getScore(String tweet) {
		double tweet_score = 0.0;
		String[] words = tweet.split("\\s+");
		for (String word : words) {
			if (logisticRegression.weights.containsKey(word))
				tweet_score += logisticRegression.weights.get(word);

			Logger.info(tweet_score + "");
		}

		return tweet_score * 10;
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
		String sql = "select * from ratings where user_id =" + uid;
		ResultSet rs = dbinstance.executeQuery(sql);
		double average = 0.0;
		int count = 0;
		while (rs.next()) {
			// System.out.println(rs.getDouble(3));
			average = average + rs.getDouble(4);
			count++;
		}
		average = average / count;
		sql = "update users set user_weight = " + average + " where user_id ="
				+ uid;
		dbinstance.updateQuery(sql);
		System.out.println("User weight updated!!");
	}

	public void updateMovieRating(int mid) throws SQLException {
		String sql = "select * from ratings join users on ratings.user_id = users.user_id and ratings.movie_id="
				+ mid;
		ResultSet rs = dbinstance.executeQuery(sql);
		double weightedsum = 0.0;
		double weights = 0.0;
		double weightedavg = 0.0;
		while (rs.next()) {
			double weight = rs.getDouble(6);
			weights = weights + weight;
			double ratings = rs.getDouble(3);
			weightedsum = weightedsum + (weight * ratings);
		}
		if (weights != 0)
			weightedavg = weightedsum / weights;
		String sql2 = "update movies set rating = " + weightedavg
				+ " where movie_id = " + mid;
		dbinstance.updateQuery(sql2);
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
