package sjtu.omnilab.kalin.stlab

/**
 * Created by chenxm on 12/24/14.
 */
object STUtils {

  /**
   * Convert geographic lat/lon into Cartesian points
   */
  def latlon2cartesian(p: GeoPoint): CartesianPoint = {
    val x = math.cos(p.lat) * math.cos(p.lon)
    val y = math.cos(p.lat) * math.sin(p.lon)
    val z = math.sin(p.lat)
    CartesianPoint(x, y, z, p.weight)
  }

  /* Convert Lat/Lon from degrees to radians */
  def deg2rad(latlon: Double): Double = {
    latlon * Math.PI / 180
  }

  /**
   * Convert Lat/Lon from radians to degrees
   */
  def rad2deg(latlon: Double): Double = {
    latlon * 180 / Math.PI
  }

  /**
   * Calculate geographic distance between two geopoints
   * Return distance in METERS
   */
  def distGeoPoints(p1: GeoPoint, p2: GeoPoint): Double = {
    val earthRadius = 6378137
    val rlat1 = deg2rad(p1.lat)
    val rlat2 = deg2rad(p2.lat)
    val rlon1 = deg2rad(p1.lon)
    val rlon2 = deg2rad(p2.lon)
    val c = math.acos(math.sin(rlat1)*math.sin(rlat2) +
    math.cos(rlat1)*math.cos(rlat2)*math.cos(rlon2-rlon1))
    c * earthRadius
  }
}
