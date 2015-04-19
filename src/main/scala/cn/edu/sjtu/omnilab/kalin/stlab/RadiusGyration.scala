package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import cn.edu.sjtu.omnilab.kalin.stlab.STUtils._

/**
 * Class that represents the weighted spatial point as `(lat, lon, weight)`
 * @param lat Latitude of a spatial point
 * @param lon Longitude of a spatial point
 * @param weight Weight value of a spatial point
 */
case class GeoPoint(lat: Double, lon: Double, weight: Double = 1.0) {
  override def toString: String = {
    "(%f,%f,%f)".format(lat, lon, weight)
  }
}

case class CartesianPoint(x: Double, y: Double, z: Double, weight: Double = 1.0) {
  override def toString: String = {
    "(%f,%f,%f,%f)".format(x, y, z, weight)
  }
}

/**
 * Calculate the radius of gyration of individual with multiple locations.
 */
class RadiusGyration extends Serializable {

  def radiusGyration(points: RDD[(String, GeoPoint)]): RDD[(String, Double)] = {
    
    val midpoints = (new GeoMidpoint).cartesian(points)

    points
      .join(midpoints)
      .mapValues { case (p1, p2) => distGeoPoints(p1, p2)}
      .groupByKey
      .mapValues { vs => 
        val n = vs.count(x => true) 
        val sqsum = vs.map(x => x * x).sum 
        math.sqrt(sqsum/n)
      }
  }
}
