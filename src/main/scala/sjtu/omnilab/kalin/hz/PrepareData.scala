package sjtu.omnilab.kalin.hz

/**
 * Created by chenxm on 12/21/14.
 */

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.joda.time.DateTime
import sjtu.omnilab.kalin.utils.RDDMultipleTextOutputFormat

/**
 * Prepare Hangzhou Datasets: splitting into multiple days.
 */
object PrepareData {

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
