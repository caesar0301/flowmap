package sjtu.omnilab.kalin.stlab

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