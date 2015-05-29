package cn.edu.sjtu.omnilab.flowmap.stlab

import org.apache.commons.math3.analysis.function.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class MPoint(uid: String, time: Double, location: String)

/**
 * tidy user's data points in form of (userID, time, locID);
 * remove location points without change in a short interval;
 * add extra HOME states to movement history
 */
object CleanseMob extends Serializable {

  /**
   * Sample users whose data meet principles:
   * 1. having more than 75% days of interactions in the given period
   * 2. having unique displacement larger than 2
   * 3. having more than 5 observations in observed days on average
   * 4. having more than 6 semi-hour intervals in observed days on average
   *
   * Data improvement:
   * 1. add `staying home` states for each user at 3:00 AM
   *
   * @param input
   * @return
   */
  def cleanse(input:RDD[MPoint], minDays:Double = 1, minTotalLoc:Int = 2, minDailyObs:Int=5,
              minDailyInt:Int=6, tzOffset:Int = 0, addNight:Boolean=true): RDD[MPoint] = {

    // filter out users without sufficient observations
    val validUsers = input.groupBy(_.uid).mapValues { logs => {
      val timed = logs.map { m => (m, m.time.toLong/1800, m.time.toLong/86400) }
      val dailyStat = timed.groupBy(_._3).mapValues { obs => {
        val ocnt = obs.size // observation number
        val icnt = obs.map(_._2).toArray.distinct.size // interval number
        (ocnt, icnt)
      }}
      val tdays = dailyStat.keys.size // total days
      val docnt = dailyStat.map(_._2._1).sum / tdays // observations per day
      val dicnt = dailyStat.map(_._2._1).sum / tdays // intervals per day
      val ulocs = logs.map(_.location).toArray.distinct.size // distinct location number
      (docnt, dicnt, tdays, ulocs)
    }}.filter(m => m._2._1 >= minDailyObs
      && m._2._2 >= minDailyInt
      && m._2._3 >= minDays
      && m._2._4 >= minTotalLoc)

    // group by users and tidy logs for each user
    input.keyBy(_.uid).join(validUsers)
      .map { case (uid, (log, stat)) => log }
      .groupBy(_.uid)
      .flatMap { case (user, logs) => {
        val etled = informativeCleansing(logs)
        if (addNight) // add `staying home` states for each user
          addHomeStatesForUser(etled, tzOffset)
        else
          etled
      }}
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
   * within the SAME interval, replace the less infrequent one with the other.
   * 2. If two consecutive records take the same location within the same
   * interval, remove the latter record in that interval. (unimplemented)
   *
   * Interval = 0.5 hour
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
      .groupBy(_._2) // group by 0.5-hour interval
      .flatMap { case (interval, logs) => {
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

  /**
   * Identify and add staying-home records into original movement data.
   * @param logs movement history for a user
   * @param offset offset hours of recorded time zone
   * @return
   */
  def addHomeStatesForUser(logs: Iterable[MPoint], offset: Int): Iterable[MPoint] = {
    def localHour(time: Double): Long = {time.toLong / 3600 + offset.toLong}
    def localDay(time: Double): Long = {(time.toLong / 3600 + offset.toLong) / 24}

    val home = extractHomeForUser(logs, offset)
    var added = Array[MPoint]()
    var lastDay: Long = -1

    logs.foreach(log => {
      val day = localDay(log.time)
      val day3am = (day * 24 + 3) * 3600
      if ( day != lastDay && home != null) {
        added = added :+ MPoint(log.uid, day3am-offset*3600-1, home)
        added = added :+ MPoint(log.uid, day3am-offset*3600+1, home)
      }
      lastDay = day
    })
    (added ++ logs).sortBy(_.time)
  }

  /**
   * Extract the location of home according to user's movement history.
   * Home is identified by the most visited locations during 22PM-07AM o'clock AM
   * @param logs movement history for a user
   * @param offset offset hours of recorded time zone
   * @return
   */
  def extractHomeForUser(logs: Iterable[MPoint], offset: Int): String = {
    val mean = 3
    val std = 4
    val gdist = new Gaussian(mean, std)

    val night = logs.toArray
      .map(log => (log.location, log.time.toLong / 3600)).distinct // add hourly interval
      .map(log => (log._1, (log._2 + offset) % 24)) // convert hour interval to time of day
      .filter(log => (log._2 >= 0 && log._2 <= 7) || log._2 >= 22) // include night interval

    if (night.size == 0)
      return null

    val nightStat = night.groupBy(_._1) // by location
      .mapValues(loclogs => {
        val hours = loclogs.groupBy(_._2).map { // by time of day
          case (tod, todlogs) => {
            val wn = todlogs.size * gdist.value(tod) / gdist.value(mean)
            (tod, wn) // time of day, weighted occurrence
          }
        }
        val n = hours.map(_._2).sum // total occurrence at night
        val mt = 1.0 * hours.map(x => x._1 * x._2).sum / n
        (mt, n) // mean occurrence time of day
      }).map( x => (x._1, x._2._1, x._2._2))

    val total: Double = nightStat.map(_._3).sum
    val homes = nightStat.map(x => (x._1, x._2, x._3, x._3 / total)) // location, mean time, N, prob
    val maxprob = homes.map(_._4).max
    homes.filter(_._4 >= maxprob).toArray.apply(0)._1
  }
}