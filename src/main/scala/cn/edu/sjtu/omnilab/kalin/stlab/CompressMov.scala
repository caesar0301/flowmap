package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

case class STPoint(uid: String, time: Double, loc: String)
case class STPoint2(uid: String, stime: Double, etime: Double, loc: String)

/**
 * Compress individual's movement in form of (UserID, Time, LocID);
 * Merge continuous records into sessions as (UserID, STime, ETime, LocID).
 *
 * All timestamps are in seconds and the default gap is six hours.
 *
 * Created by chenxm on 4/7/15.
 */
class CompressMov extends Serializable {

  def doCompress(input: RDD[STPoint], gap: Double = 6 * 3600):
    RDD[STPoint2] = {

    // group by users and merge duplicated locations
    val compressed = input.groupBy(_.uid)
    .flatMap { case (user, logs) => {
      val compressedLogs = ListBuffer[(Double, Double, String)]()
      var lastLoc: String = null
      var lastTime: Double = -1
      var curLoc: String = null
      var curTime: Double = -1
      var stime: Double = -1

      for (log <- logs) {
        curLoc = log.loc
        curTime = log.time

        if ( lastLoc == null ) {
          stime = curTime
        } else {
          if ( curLoc != lastLoc || (curLoc == lastLoc && curTime - lastTime >= gap )) {
            // add a new session when the location changes
            // or the gap is larger than a given threshold
            compressedLogs += Tuple3(stime, lastTime, lastLoc)
            stime = curTime
          }
        }

        lastLoc = curLoc
        lastTime = curTime
      }

      compressedLogs.map( value => STPoint2(user, value._1, value._2, value._3))
    }}

    compressed
  }
}
