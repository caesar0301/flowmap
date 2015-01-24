package cn.edu.sjtu.omnilab.kalin.hz

/**
 * The input data are based on the output of `hzm_etl.pig` script at
 * https://github.com/caesar0301/paper-mstd-code/blob/master/datatools/hzm_etl.pig
 */

import cn.edu.sjtu.omnilab.kalin.utils.RDDMultipleTextOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.joda.time.DateTime

/**
 * Prepare Hangzhou Datasets: splitting into multiple days.
 */
object PrepareDataJob {

  /**
   * Main portal.
   * @param args
   * @return
   */
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Prepare Hangzhou Datasets")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    val logs = sc.textFile(input)

    logs
      .map(_.split("\t"))
      .keyBy(line => parseDay((line(0).toDouble * 1000).round))
      .mapValues(x => x.mkString("\t"))
      .partitionBy(new HashPartitioner(32))
      .saveAsHadoopFile(output, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])

    sc.stop()
  }

  /**
   * Parse date value as file name from recording time.
   * @param milliSecond
   * @return
   */
  def parseDay(milliSecond: Long): String = {
    val datetime = new DateTime(milliSecond)
    return "SET%02d%02d".format(datetime.getMonthOfYear, datetime.getDayOfMonth)
  }
}
