package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import cn.edu.sjtu.omnilab.kalin.stlab.STUtils._

/**
 * Created by chenxm on 12/23/14.
 *
 * Class to calculate the geographic midpoint (a.k.a. geographic center,
 * or center of gravity). Algorithms here are inspired by the calculation
 * of GeoMidpoint.com (http://www.geomidpoint.com/calculation.html).
 */
class GeoMidpoint extends Serializable {

  /**
   * This method finds a simple average latitude and longitude for the
   * locations in 'Your Places'. This is equivalent to finding a midpoint on
   * a flat rectangular projection map. When the distance between locations
   * is less than 250 miles (400 km), this method gives a close approximation
   * to the true geographic midpoint in Method cartesian().
   * @param points
   * @return
   */
  def avgLatLon(points: RDD[(String, GeoPoint)]): RDD[(String, GeoPoint)] = {
    
    // eliminate the problems of the International Date Line
    val carMidPoints = cartesian(points).cache()

    // convert degrees to radius coordinates
    val midPoints = points.join(carMidPoints).map {
      case (user, (point, mp)) =>
        val lat = point.lat
        // relative longitudes to the midpoint
        var rlon = point.lon - mp.lon
        if (rlon > 180) rlon -= 360
        else if (rlon < -180) rlon += 360
        (user, (GeoPoint(lat, rlon, point.weight), mp.lon))
    }.mapValues { case (point, mplon) =>
      (GeoPoint(
        point.lat * point.weight,
        point.lon * point.weight,
        point.weight),
        mplon)
    }.reduceByKey { case ((p1, mplon1), (p2, mplon2)) =>
      val lat = p1.lat + p2.lat
      val rlon = p1.lon + p2.lon
      val weight = p1.weight + p2.weight
      (GeoPoint(lat, rlon, weight), mplon1)
    }.mapValues { case (p, mplon) =>
      val lat = p.lat / p.weight
      var lon = p.lon / p.weight + mplon
      if (lon > 180) lon -= 360
      else if (lon < -180) lon += 360
      GeoPoint(lat, lon)
    }
    
    midPoints
  }

  /**
   * Calculate the geographic midpoint with Cartesian coordinates.
   * @param points RDD[UserID: String, _: GeoPoint]
   * @return calculated midpoint, RDD[UserID: String, _: CartesianPoint]
   *         the weight value takes default value == 1.
   */
  def cartesian(points: RDD[(String, GeoPoint)]): RDD[(String, GeoPoint)] = {
    
    // convert lat/lon to cartesian coordinates
    val carPoints = points.map { case (user, point) =>
      val lat = deg2rad(point.lat)
      val lon = deg2rad(point.lon)
      val cp = latlon2cartesian(GeoPoint(lat, lon, point.weight))
      (user, cp)
    }

    // calculate weighted average Cartesian coordinates
    val midPoints = carPoints.mapValues { cp =>
      val wx = cp.x * cp.weight
      val wy = cp.y * cp.weight
      val wz = cp.z * cp.weight
      CartesianPoint(wx, wy, wz, cp.weight)
    }.reduceByKey { (p1, p2) =>
      CartesianPoint(
        p1.x + p2.x,
        p1.y + p2.y,
        p1.z + p2.z,
        p1.weight + p2.weight
      )
    }.mapValues { cp =>
      val x = cp.x / cp.weight
      val y = cp.y / cp.weight
      val z = cp.z / cp.weight
      val lon = math.atan2(y, x)
      val hyp = math.sqrt(x * x + y * y)
      val lat = math.atan2(z, hyp)
      GeoPoint(rad2deg(lat), rad2deg(lon))
    }

    midPoints
  }

}