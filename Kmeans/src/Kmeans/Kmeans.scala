/**
 * Implementation of Kmeans algorithm
 */
package Kmeans
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.util.Vector

/**
 * @author Ekal.Golas
 *
 */
object Kmeans {
  def main(args: Array[String]) {
    // Get the text mapped to x,y coordinates
    val conf = new SparkConf().setAppName("Kmeans")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val points = sc.textFile(args(0), 2).cache().map(x => new Vector(x.split(" ").map { x => x.toDouble }))

    // Set K and number of iterations
    val K = 3
    var iterations = 10

    // Take sample of k points
    var sample = points.takeSample(false, K, 1)

    // Repeat for the number of iterations
    while (iterations > 0) {
      // Get the nearest points to the initial sample centroids taken
      val nearest = points.map(x => (closestPoint(x, sample), (x, 1)))

      // Sum all vectors and the counts (Counts are needed for the mean)
      val pointSum = nearest.reduceByKey {
        case ((x1, y1), (x2, y2)) =>
          (x1 + x2, y1 + y2)
      }

      // For each index (1), collect its mean (sum(2.1) / count(2.2))
      val pointMean = pointSum.map { x => (x._1, x._2._1 / x._2._2) }.collectAsMap()

      // Set the calculated mean (2) to the current sample index (1)
      for (newSample <- pointMean)
        sample(newSample._1) = newSample._2

      // Decrement iterations
      iterations -= 1;
    }

    // Get corresponding clusters for the data points and print them
    println("Corresponding cluster: (Point)")
    val nearest = points.map(p => (closestPoint(p, sample), p))
    nearest.foreach(x => println(x._1 + ": " + x._2.toString()))

    // Print the final centroids
    println("\nFinal centroids: ")
    sample.foreach(println)
  }

  /**
   * Gets the index of the closest centroid to the point
   * @param point Point from which distance should be fetched
   * @param centroids Array of vectors of centroids
   * @return Index of the closest centroids
   */
  def closestPoint(point: Vector, centroids: Array[Vector]) = {
    // Initialize variables
    var index = 0
    var closest = Double.PositiveInfinity

    // Process each centroid
    for (i <- 0 until centroids.length) {
      // Calculate the distance of the point from the centroid
      var distance = point.squaredDist(centroids(i))

      // If this distance is lesser than closest distance, set it as closest
      if (distance < closest) {
        closest = distance
        index = i
      }
    }

    // Return the index found
    index
  }
}