package cn.edu.sjtu.omnilab.kalin.d4d

import cn.edu.sjtu.omnilab.kalin.stlab.{STUtils, MPoint, TidyMovement}
import org.apache.spark.{SparkContext, SparkConf}

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
      val time = STUtils.ISOToUnix(parts(1)) / 1000.0
      val loc = parts(2)
      MPoint(uid, time, loc)
    })
    
    val tidyMove = new TidyMovement().tidy(formatedRDD)
    tidyMove
      .sortBy(tuple => (tuple.uid, tuple.time))
      .map(tuple => "%s,%.3f,%s".format(tuple.uid, tuple.time, tuple.location))
      .saveAsTextFile(output)
    
    spark.stop()
  }
}
