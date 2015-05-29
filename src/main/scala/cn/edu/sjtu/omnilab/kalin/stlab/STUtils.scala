package cn.edu.sjtu.omnilab.kalin.stlab

import java.util.Locale

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat

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

  /**
   * Converts ISO8601 datetime strings to Unix Time Longs (milliseconds)
   * @param isotime
   * @return
   */
  def ISOToUnix(isotime: String): Long = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    fmt.parseDateTime(isotime).getMillis
  }

  /**
   * Converts Unix Time Longs (milliseconds) to ISO8601 datetime strings
   * @param unixtime
   * @return
   */
  def UnixToISO(unixtime: Long): String = {
    new DateTime(unixtime).formatted("yyyy-MM-dd HH:mm:ss")
  }
}
