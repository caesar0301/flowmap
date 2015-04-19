package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class MPoint(uid: String, time: Double, location: String)

/**
 * tidy user's data points in form of (userID, time, locID);
 * remove location points without change in a short interval.
 */
class TidyMovement extends Serializable {

  /**
   * Sample users whose data meet principles:
   * 1. having more than 75% days of interactions in the given period
   * 2. having unique displacement larger than 2
   * 3. having more than 5 observations in observed days on average
   * 4. having more than 6 semi-hour intervals in observed days on average
   *
   * @param input
   * @return
   */
  def tidy(input: RDD[MPoint], minDays: Double = 14 * 0.75): RDD[MPoint] = {

    val minTotalLoc = 2
    val minDailyObs = 5
    val minDailyInt = 6

    // filter out users without sufficient observations
    val validUsers = input.groupBy(_.uid).mapValues { logs => {
      val timed = logs.map { m => (m, m.time.toLong/1800, m.time.toLong/86400) }

      val dailyStat = timed.groupBy(_._3).mapValues { obs => {
        val ocnt = obs.size
        val icnt = obs.map(_._2).toArray.distinct.size
        (ocnt, icnt)
      }}

      val tdays = dailyStat.keys.size
      val docnt = dailyStat.map(_._2._1).sum / tdays
      val dicnt = dailyStat.map(_._2._1).sum / tdays
      val ulocs = logs.map(_.location).toArray.distinct.size
      (docnt, dicnt, tdays, ulocs)
    }}.filter(m => m._2._1 >= minDailyObs && m._2._2 >= minDailyInt
      && m._2._3 >= minDays && m._2._4 >= minTotalLoc)

    // group by users and tidy logs for each user
    input.keyBy(_.uid).join(validUsers).map { case (uid, (m, t)) => m }
      .groupBy(_.uid).flatMap { case (user, logs) => informativeCleansing(logs) }
  }

  /**
   * Remove redundant movement data if two consecutive logs record the same location
   * within `interval` seconds.
   *
   * @param logsPerUser
   * @return
   */
  def simpleCleansing(logsPerUser: Iterable[MPoint]): Iterable[MPoint] = {

    // the temporal tolerance (seconds) to combine two logs
    val interval = 60
    val logBuf = ListBuffer[MPoint]()
    val tidyLogs = ListBuffer[MPoint]()

    // clear redundant movement in a buffer
    def clearBuffer(buf: ListBuffer[MPoint]): Unit = {
      var lastBS: String = null
      for ( i <- buf ){
        if( null == lastBS || i.location != lastBS )
          tidyLogs += i
        lastBS = i.location
      }
      buf.clear()
    }

    logsPerUser.foreach( log => {
      if (logBuf.length > 0) {
        // clear buffer if time interval larger than one minute
        val timeDiff = log.time - logBuf(0).time
        if (timeDiff >= interval )
          clearBuffer(logBuf)
      }
      logBuf += log
    })

    clearBuffer(logBuf)
    tidyLogs
  }

  /**
   * Cleanse individual movement data according to informative principles.
   *
   * 1. If more than 3 transitions are present for two locations
   * within the same interval, replace the less infrequent one with the other.
   * 2. If two consecutive records take the same location within the same
   * interval, remove the latter record in that interval.
   *
   * @param logsPerUser
   * @return
   */
  def informativeCleansing(logsPerUser: Iterable[MPoint]): Iterable[MPoint] = {

    val transitionThreshold = 3
    val total = logsPerUser.size
    // location freqency stat for location ordering
    val locStat = logsPerUser.groupBy(_.location).map { case (loc, logs) => (loc, 1.0*logs.size/total) }

    logsPerUser.map ( m => (m, m.time.toLong/1800) )
      .groupBy(_._2).flatMap { case (int, logs) => { // group by 0.5-hour interval

        // calculate transition stat and filter out infrequent pairs
        var trans = new mutable.HashMap[(String, String), Long]
        var preLoc: String = null
        logs.foreach { case (m, int) => {
          if ( preLoc != null && m.location != preLoc ) {
            var key = (preLoc, m.location)
            if ( locStat.get(preLoc).get > locStat.get(m.location).get)
              key = (m.location, preLoc)
            if ( ! trans.contains(key) )
              trans.put(key, 1)
            else
              trans.put(key, trans.get(key).get + 1)
          }
          preLoc = m.location
        }}

        val vtrans = trans.filter(_._2 >= transitionThreshold).keySet.toMap

        // replace the less infrequent location with the other one
        preLoc = null
        var result = new Array[MPoint](0)
        logs.foreach { case (m, int) => {
          val uid = m.uid
          val time = m.time
          val loc = vtrans.get(m.location).getOrElse(m.location)
          if ( loc != preLoc )
            result = result :+ MPoint(uid, time, loc)
          preLoc = loc
        }}

        result
      }
    }
  }

}