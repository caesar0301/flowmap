package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by chenxm on 12/22/14.
 */
object CountLogs {
  def main (args: Array[String]) {
    val usage = "Usage: CountLogs <input>"
    require((args(0)).length > 0, usage)

    val conf = new SparkConf().setAppName("Count log numbers")
    val spark = new SparkContext(conf)

    val cnt = spark.textFile(args(0)).count()
    println("Total logs: " + cnt)

    spark.stop()
  }
}
