package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * tidy user's data points in form of (userID, time, locID);
 * remove location points without change in a short interval.
 */
class TidyMovement extends Serializable {

  def tidy(input: RDD[String]): RDD[(String, Double, Long)] = {

    val logs = input.map(_.split("\t"))
      .sortBy(f => f(DataSchema.TTime))
      // select fields indicating movement
      .map { line =>
        val imsi = line(DataSchema.IMSI)
        val ttime = line(DataSchema.TTime).toDouble
        val bs = line(DataSchema.BS).toLong
        (imsi, ttime, bs)
      }

    // group by users and tidy logs for each user
    val tidyLogRDD = logs.groupBy(_._1)
      .map { case (user, logs) => {
        val movement = compressMovement(logs)
        (user, movement)
      }}

    tidyLogRDD
      .flatMap {
        case (user, tuples) =>
          tuples.map( value => value)
      }
  }
  
  private def compressMovement(logsPerUser: Iterable[(String, Double, Long)])
  : Array[(String, Double, Long)] = {

    val logBuf = ListBuffer[(String, Double, Long)]()
    val tidyLogs = ListBuffer[(String, Double, Long)]()

    // clear redundant movement in a buffer
    def clearBuffer(buf: ListBuffer[(String, Double, Long)]): Unit = {
      var lastBS: Long = -1
      for ( i <- buf ){
        if( lastBS == -1 || i._3 != lastBS )
          tidyLogs += i
        lastBS = i._3
      }
      buf.clear()
    }

    for ( log <- logsPerUser ){

      if (logBuf.length > 0) {
        // clear buffer if time interval larger than one minute
        val timeDiff = log._2 - logBuf(0)._2
        if (timeDiff >= 60 )
          clearBuffer(logBuf)
      }

      logBuf += log
      
    }
    
    // process left logs in buffer
    clearBuffer(logBuf)
    
    tidyLogs.toArray
  }

}