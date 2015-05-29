package cn.edu.sjtu.omnilab.kalin.cc

import cn.edu.sjtu.omnilab.kalin.stlab._
import org.apache.spark.{SparkConf, SparkContext}

object GenFlowmap {

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: GenFlowmap <in> <out> [interval] [minnum]")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    var minnum = 5
    var interval = 3600 * 6
    if (args.size >= 3)
      interval = args(2).toInt
    if (args.size >= 4)
      minnum = args(3).toInt
    
    val conf =  new SparkConf().setAppName("Generate flowmap")
    val spark = new SparkContext(conf)
    
    val inputRDD = spark.textFile(input).map(_.split(",")).cache
    
    val formatedRDD = inputRDD.map(parts => {
      val uid = parts(0)
      val time = parts(1).toDouble
      val loc = parts(2)
      STPoint(uid, time, loc)
    })
    
    Flowmap.draw(formatedRDD, interval)
      .filter(_.NumUnique >= minnum)
      .sortBy(m => (m.interval, m.FROM, m.TO))
      .map(m => "%d,%s,%s,%d,%d".format(m.interval, m.FROM, m.TO, m.NumTotal, m.NumUnique))
      .saveAsTextFile(output)
    
    spark.stop()
  }
}