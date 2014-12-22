package sjtu.omnilab.kalin.hz

/**
 * Created by chenxm on 12/21/14.
 */

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.joda.time.DateTime

/**
 * Output RDD into multiple files named by the key.
 */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}

/**
 * Prepare Hangzhou Datasets: splitting into multiple days.
 */
object PrepareData {

  /**
   * Parse date value as file name from recording time.
   * @param milliSecond
   * @return
   */
  def parseDay(milliSecond: Long): String = {
    val datetime = new DateTime(milliSecond)
    return "SET%02d%02d".format(datetime.getMonthOfYear, datetime.getDayOfMonth)
  }

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
}
