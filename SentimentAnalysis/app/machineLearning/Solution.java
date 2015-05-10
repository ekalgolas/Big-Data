package machineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Class to implement classification of movie reviews
 *
 * @author Ekal.Golas
 *
 */
public class Solution {
	/**
	 * Initialize hash set for total unique words
	 */
	public static HashSet<String> words = new HashSet<>();

	/**
	 * Parses the arguments and trains the classifiers
	 *
	 * @param args
	 *            Program arguments
	 * @return array of decimals, denoting naive bayes and logisitc regression
	 *         accuracies
	 * @throws IOException
	 */
	public static double[] classify(String[] args) throws IOException {
		// Initialize array to store accuracies
		double[] accuracies = new double[2];

		// Parse arguments
		double lambda = Double.parseDouble(args[0]);
		double eta = Double.parseDouble(args[1]);
		int limitIterations = Integer.parseInt(args[2]);
		String train_folder = args[3];
		String test_folder = args[4];

		// Parse the features from spam and ham folder
		ArrayList<ArrayList<String>> spam = Parser.parseFolder(train_folder
				+ "\\spam");
		ArrayList<ArrayList<String>> ham = Parser.parseFolder(train_folder
				+ "\\ham");
		ArrayList<ArrayList<String>> test_spam = Parser.parseFolder(test_folder
				+ "\\spam");
		ArrayList<ArrayList<String>> test_ham = Parser.parseFolder(test_folder
				+ "\\ham");

		util.Rating.logisticRegression.train(ham, spam, lambda,
				limitIterations, eta);

		// Evaluate the classifiers
		accuracies[0] = util.Rating.logisticRegression.getAccuracy(test_ham,
				test_spam);

		// Return the accuracies
		return accuracies;
	}
}