/**
 * Reads the static IMDB data and transforms it
 */
package Data
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }

import java.io.File

/**
 * @author Ekal.Golas
 *
 */
object StaticDataReader {
	def main(args : Array[String]) {
		// Get a map of words and weights
		var vocab : Map[String, Double] = Map()
		for (key <- Source.fromFile(args(0))("UTF-8").getLines; value <- Source.fromFile(args(1))("UTF-8").getLines)
			vocab += (key -> value.toDouble)

		vocab.foreach(println)
	}
}