package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

/**
 * Export Hangzhou mobile data in the long format.
 */
object TidyMovementJob {
  
  def main(args: Array[String]) {

    if (args.length != 2){
      println("Usage: TidyDataPointsJob <LOGSET> <LFDATA>")
      sys.exit(0)
    }

    val input = args(0)
    val output = args(1)

    // configure spark
    val conf = new SparkConf()
    conf.setAppName("Tidy movement data for briefness")
    val spark = new SparkContext(conf)

    // read logs from data warehouse
    val inputRDD = spark.textFile(input).cache

    // unique user IDs
    val userID = inputRDD.map( line => line.split("\t"))
      .map( _(DataSchema.IMSI) )
      .distinct
      .zipWithUniqueId // (imsi, userID) 
      .cache

    // geographic location of base stations
    val cellID = inputRDD.map( line => line.split("\t"))
      .map( _(DataSchema.BS).toLong )
      .distinct
      .zipWithUniqueId // (bs, cellID)
      .cache

    // compress movement history: (imsi, time, baseStation)
    val cleaned = new TidyMovement().tidy(inputRDD)

    // smash user identities by regenerating ids and add location lon/lat
    cleaned.keyBy(_._1)
      .join(userID).values
      .map { case ((imsi, time, bs), (userID)) => (bs, (time, userID)) }
      .join(cellID).values
      .map { case ((time, userID), (cellID)) =>
        "%d,%.03f,%d".format(userID, time, cellID) }
      .saveAsTextFile(output)

    // stop spark engine
    spark.stop()
    
  }

}