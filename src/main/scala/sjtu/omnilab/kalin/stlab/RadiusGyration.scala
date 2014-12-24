package sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import sjtu.omnilab.kalin.stlab.STUtils._

/**
 * Calculate the radius of gyration of individual with multiple locations.
 */
class RadiusGyration extends Serializable {

  def radiusGyration(points: RDD[(String, GeoPoint)]): RDD[(String, Double)] = {
    val midpoints = (new GeoMidpoint).cartesian(points)

    points.join(midpoints).mapValues { case (p1, p2) =>
      distGeoPoints(p1, p2)
    }.groupByKey.mapValues { vs =>
      val n = vs.count(x => true)
      val sqsum = vs.map(x => x * x).sum
      math.sqrt(sqsum/n)
    }
  }
}
