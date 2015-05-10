import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import machineLearning.Parser;
import play.GlobalSettings;
import play.Application;
import play.Logger;
import util.Rating;

/**
 * Class to initialize machine learning classifier
 * 
 * @author Ekal.Golas
 *
 */
public class Global extends GlobalSettings {
	/*
	 * (non-Javadoc)
	 * 
	 * @see play.GlobalSettings#onStart(play.Application)
	 */
	public void onStart(Application app) {
		Logger.info("Training the system from IMDB static dataset.....");
		File f = new File("dataset/bag.txt");
		if (f.exists()) {
			try (BufferedReader br = new BufferedReader(new FileReader(
					"dataset/bag.txt"))) {
				String line = br.readLine();
				while (line != null) {
					// System.out.println(line);
					String[] params = line.split("\t");
					Rating.logisticRegression.weights.put(params[0],
							Double.parseDouble(params[1]));
					line = br.readLine();
				}
			} catch (Exception IOException) {
				System.out.println("Exception caught : " + IOException);
			}
		} else {
			String train_folder = "D:\\UTD\\Class notes\\Big Data\\Assignment\\project\\aclImdb\\train";
			try {
				// Parse the features from neg and pos folder
				ArrayList<ArrayList<String>> neg = Parser
						.parseFolder(train_folder + "\\neg");
				ArrayList<ArrayList<String>> pos = Parser
						.parseFolder(train_folder + "\\pos");

				Rating.logisticRegression.train(pos, neg, 0.1, 50, 0.001);

				// train_folder =
				// "D:\\UTD\\Class notes\\Big Data\\Assignment\\project\\aclImdb\\test";
				// neg = Parser.parseFolder(train_folder + "\\neg");
				// pos = Parser.parseFolder(train_folder + "\\pos");

				// Rating.logisticRegression.train(pos, neg, 0.1, 100, 0.001);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try (BufferedWriter br = new BufferedWriter(new FileWriter(f.getPath()))) {
			for (String word : Rating.logisticRegression.weights.keySet())
				br.write(word + "\t"
						+ Rating.logisticRegression.weights.get(word) + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Logger.info("System trained!!!.....");
	}
}