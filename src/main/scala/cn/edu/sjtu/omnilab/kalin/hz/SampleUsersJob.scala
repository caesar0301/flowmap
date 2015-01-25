package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

/**
 * Sample users whose data meets two scenarios:
 * 1. having more than 75% days of interactions in given period
 * 2. having unique displacement larger than 2
 */
object SampleUsersJob {
  
  val totalDays = 17
  val deadline = 0.75

  def main(args: Array[String]) {
    if ( args.length < 2 ) {
      println("usage: SampleUsersJob <in> <out>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    
    val conf = new SparkConf()
    conf.setAppName("Sample users of high data quality")
    val sc = new SparkContext(conf)
    
    val inputRDD = sc.textFile(input)
    .map { line => {
      val parts = line.split(",")
      (parts(0), parts(1).toDouble, parts(2))
    }}
    
    val users = inputRDD.groupBy(_._1) // group by user
    .filter { case (key, value) => selectUserOrNot(value.toArray) }
      .flatMap { case (k, v) => v }
      .map(v => "%s,%.3f,%s".format(v._1, v._2, v._3) )
      .saveAsTextFile(output)
  }
  
  def selectUserOrNot(movement: Array[(String, Double, String)]) = {
    val distinctDays = movement.map( tuple => {
      val datetime = new DateTime((tuple._2 * 1000).toLong)
      val year = datetime.getYear
      val month = datetime.getMonthOfYear
      val day = datetime.getDayOfMonth
      (year, month, day)
    }).distinct
    
    val distinctCells = movement.map( tuple => tuple._3)
      .distinct.length
    
    var selected = false
    if ( 1.0 * distinctDays.length / totalDays >= deadline
      && distinctCells >= 2)
      selected = true
    
    selected
  }
  
}
