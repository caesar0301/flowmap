package cn.edu.sjtu.omnilab.kalin.d4d

import cn.edu.sjtu.omnilab.kalin.stlab.TidyMovement
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object TidyMovementJob {

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: TidyMovementJob <in> <out>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    
    val conf =  new SparkConf().setAppName("Tidy D4D movement history")
    val spark = new SparkContext(conf)
    
    val inputRDD = spark.textFile(input).map(_.split(",")).cache
    
    val formatedRDD = inputRDD.map(parts => {
      val uid = parts(0)
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      val time = fmt.parseDateTime(parts(1)).getMillis / 1000.0
      val loc = parts(2)
      (uid, time, loc)
    })
    
    val tidyMove = new TidyMovement().tidy(formatedRDD)
    tidyMove
      .sortBy(tuple => (tuple._1, tuple._2))
      .map(tuple => "%s,%.3f,%s".format(tuple._1, tuple._2, tuple._3))
      .saveAsTextFile(output)
    
    spark.stop()
  }
}
