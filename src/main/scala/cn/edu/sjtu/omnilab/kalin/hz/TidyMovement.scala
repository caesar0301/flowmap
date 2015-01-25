package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * tidy user's data points in form of (userID, time, locID);
 * remove location points without change in a short interval.
 */
class TidyMovement extends Serializable {

  def tidy(input: RDD[(String, Double, String)], interval: Int = 60)
  : RDD[(String, Double, String)] = {

    // group by users and tidy logs for each user
    val tidyLogRDD = input.groupBy(_._1)
      .map { case (user, logs) => {
        val movement = compressMovement(logs, interval)
        (user, movement)
      }}

    tidyLogRDD
      .flatMap {
        case (user, tuples) =>
          tuples.map( value => value)
      }
  }
  
  private def compressMovement(logsPerUser: Iterable[(String, Double, String)],
                                interval: Int)
  : Array[(String, Double, String)] = {

    val logBuf = ListBuffer[(String, Double, String)]()
    val tidyLogs = ListBuffer[(String, Double, String)]()

    // clear redundant movement in a buffer
    def clearBuffer(buf: ListBuffer[(String, Double, String)]): Unit = {
      var lastBS: String = null
      for ( i <- buf ){
        if( null == lastBS || i._3 != lastBS )
          tidyLogs += i
        lastBS = i._3
      }
      buf.clear()
    }

    for ( log <- logsPerUser ){

      if (logBuf.length > 0) {
        // clear buffer if time interval larger than one minute
        val timeDiff = log._2 - logBuf(0)._2
        if (timeDiff >= interval )
          clearBuffer(logBuf)
      }

      logBuf += log
      
    }
    
    // process left logs in buffer
    clearBuffer(logBuf)
    
    tidyLogs.toArray
  }

}