package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

/**
 * Compress individual's movement in form of (UserID, Time, LocID);
 * Merge continuous records into sessions as (UserID, STime, ETime, LocID).
 *
 * All timestamps are in seconds and the default gap is six hours.
 *
 * Created by chenxm on 4/7/15.
 */
class CompressMov extends Serializable {

  def doCompress(input: RDD[(String, Double, String)], gap: Double = 6 * 3600)
  : RDD[(String, Double, Double, String)] = {

    // group by users and merge duplicated locations
    val compressed = input.groupBy(_._1)
    .flatMap { case (user, logs) => {
      val compressedLogs = ListBuffer[(Double, Double, String)]()
      var lastLoc: String = null
      var lastTime: Double = -1
      var curLoc: String = null
      var curTime: Double = -1
      var stime: Double = -1

      for (log <- logs) {
        curLoc = log._3
        curTime = log._2

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

      compressedLogs.map( value => (user, value._1, value._2, value._3))
    }}

    compressed
  }
}
