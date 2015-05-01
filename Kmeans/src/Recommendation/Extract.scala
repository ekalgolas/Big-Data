/**
 * Extract the movie IDs from item-similarity matrix file and generate a matrix
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
object Extract {
  def main(args: Array[String]) {
    // Get all the users mapped
    val conf = new SparkConf().setAppName("Extract")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val users = sc.textFile(args(0), 2).cache().map(_.split("::"))

    // Filter the user we need
    val user = users.filter(array => array(0) == args(2))

    // create an empty map
    val itemsimilarity = sc.textFile(args(1), 2).cache().map(_.split("\t"))

    // Get the movie for the user
    val movie = user.map { x => x(1) }

    // Get similar movies
    val array = movie.toArray()
    val similar = itemsimilarity.filter { x => array.contains(x(0)) }
    val movies = similar.map { x => x.mkString("\t").split(" ") }
    val matrix = movies.map { x => x.map { _.split(":")(0) } }

    // Print the matrix
    println("Matrix for user " + args(2) + " is generated as:")
    matrix.foreach { x => println(x.mkString(",")) }
  }
}