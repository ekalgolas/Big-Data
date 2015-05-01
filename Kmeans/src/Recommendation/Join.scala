/**
 * Replace movie ID with movie name in the matrix
 */
package Recommendation
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * @author Ekal.Golas
 *
 */
object Join {
  def main(args: Array[String]) {
    // Get all the users mapped
    val conf = new SparkConf().setAppName("Join")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val users = sc.textFile(args(0), 2).cache().map(_.split("::"))

    // Get the movies mapped
    val movieNames = sc.textFile(args(1), 2).cache().map(_.split("::"))
    val movieMap: Map[String, String] = movieNames.map(x => (x(0), x(1))).collect().toMap

    // Filter the user we need
    val user = users.filter(array => array(0) == args(3))

    // create an empty map
    val itemsimilarity = sc.textFile(args(2), 2).cache().map(_.split("\t"))

    // Get the movie for the user
    val movie = user.map { x => x(1) }

    // Get similar movies
    val array = movie.toArray()
    val similar = itemsimilarity.filter { x => array.contains(x(0)) }
    val movies = similar.map { x => x.mkString("\t").split(" ") }
    val matrix = movies.map { x => x.map { _.split(":")(0) } }

    // Map the matrix and transform it
    val matrixMap = matrix.map { x => x.mkString(",").split("\t") }
    val result = matrixMap.map { x =>
      {
        x(0) = movieMap(x(0))
        x(1) = x(1).split(",").map { x => x + ":" + movieMap(x) }.mkString(",")
        x
      }
    }

    // Print the result
    result.foreach { x => println(x.mkString("\t")) }
  }
}