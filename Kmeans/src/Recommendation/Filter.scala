/**
 * Finds all the movies that user rates with rating 3
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
object Filter {
  def main(args: Array[String]) {
    // Get all the users mapped
    val conf = new SparkConf().setAppName("Filter")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val users = sc.textFile(args(0), 2).cache().map(_.split("::"))
    
    // Filter the user we need
    val user = users.filter(array => array(0) == args(1))
    
    // Print the movies for the user
    println("Movies for the user " + args(1) + " are:")
    user.foreach { x => println(x(1)) }
  }
}