package machineLearning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import play.Logger;

/**
 * Class which implements Logistic Regression classifier
 *
 * @author Ekal.Golas
 *
 */
public class LogisticRegression {
	/**
	 * W0 weight in the equation
	 */
	public double W0 = 1.0;

	/**
	 * Map that contains string to decimal pairs
	 */
	public HashMap<String, Double> weights;

	/**
	 * Default constructor
	 */
	public LogisticRegression() {
		// Map weights
		weights = new HashMap<>();
		Iterator<String> iterator = Solution.words.iterator();
		while (iterator.hasNext())
			weights.put(iterator.next(), 0.0);
	}

	/**
	 * Maps the data to a hashmap of string-integer
	 *
	 * @param data
	 *            Data to be mapped
	 * @param map
	 *            Variable to be mapped into
	 * @return Count of features
	 */
	private void map(ArrayList<ArrayList<String>> data) {
		// Map features in each document
		for (ArrayList<String> features : data)
			for (String feature : features) {
				// Add it to the set of words
				Solution.words.add(feature);
			}
	}

	/**
	 * @Calculates the Y in the learning equation
	 * 
	 * @param features
	 *            Features to process
	 * @param map
	 *            Map to look up from
	 * @param initial
	 *            Initial value of the sum
	 * @return Y as decimal
	 */
	private double getY(ArrayList<String> features,
			HashMap<String, Double> map, double initial) {
		// Get sum of weights
		double total = initial;
		for (String feature : features) {
			total += map.containsKey(feature) ? map.get(feature) : 0.0;

			if (total > 709.0)
				return 1.0;
			else if (total < -700.0)
				return -1.0;
			else if (Double.isNaN(total))
				return 0.0;
		}

		// If total is too big for exp function
		if (total > 709.0)
			return 1.0;
		else if (total < -700.0)
			return -1.0;
		else if (Double.isNaN(total))
			return 0.0;
		else {
			// Else return the value computed from the equation
			return (Math.exp(total + W0 - initial) / (1.0 + Math.exp(total + W0
					- initial)));
		}
	}

	/**
	 * Trains the classifier
	 * 
	 * @param ham
	 *            Ham data
	 * @param spam
	 *            Spam data
	 * @param lambda
	 *            Lambda parameter to penalize weights
	 * @param limit
	 *            Limit to number of iterations
	 * @param eta
	 *            Learning rate
	 */
	public void train(ArrayList<ArrayList<String>> ham,
			ArrayList<ArrayList<String>> spam, double lambda, int limit,
			double eta) {
		map(ham);
		map(spam);
		Logger.info(Solution.words.size() + "");
		Iterator<String> iterator = Solution.words.iterator();
		while (iterator.hasNext())
			weights.put(iterator.next(), 0.0);

		// Do till we reach the limit
		for (int i = 0; i < limit; i++) {
			// Set gradient
			HashMap<String, Double> gradient = new HashMap<>();
			iterator = Solution.words.iterator();
			while (iterator.hasNext())
				gradient.put(iterator.next(), 0.0);

			// Process features in spam
			for (ArrayList<String> features : spam) {
				// Get Y
				double Y = getY(features, weights, 0.0);

				// Update gradient
				for (String feature : features) {
					if (Double.isNaN(gradient.get(feature) + 1 - Y))
						break;

					gradient.put(feature, gradient.get(feature) + 1 - Y);
				}
			}

			// Process features in ham
			for (ArrayList<String> features : ham) {
				// Get Y
				double Y = getY(features, weights, 0.0);

				// Update gradient
				for (String feature : features) {
					if (Double.isNaN(gradient.get(feature) - Y))
						break;

					gradient.put(feature, gradient.get(feature) - Y);
				}
			}

			// Update weights
			for (String word : gradient.keySet()) {
				double weight = weights.get(word) + eta
						* (gradient.get(word) - (lambda * weights.get(word)));

				if (Double.isNaN(weight)) {
					Logger.info(weights.get(word) + " " + gradient.get(word));
					return;
				}

				weights.put(word, weight);
			}
		}
	}
}