package cn.edu.sjtu.omnilab.kalin.hz

import cn.edu.sjtu.omnilab.kalin.stlab.TidyMovement
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

/**
 * Export Hangzhou mobile data in the long format.
 */
object TidyMovementJob {
  
  val userMapOut = ".umap"
  val bsMapOut = ".bsmap"
  
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
    val inputRDD = spark.textFile(input)
      .map(_.split("\t"))
      .cache

    // unique user IDs
    val userID = inputRDD
      .map( _(DataSchema.IMSI) )
      .distinct
      .zipWithUniqueId // (imsi, userID) 
      .cache
    
    // save imsi-userid map for later use
    userID.map(t => "%d,%s".format(t._2, t._1))
      .saveAsTextFile(output + userMapOut)

    // geographic location of base stations
    val cellID = inputRDD
      .map { parts =>
      val bs = parts(DataSchema.BS)
      val lon = parts(DataSchema.LON)
      val lat = parts(DataSchema.LAT)
      (bs, lon, lat)
    }.distinct
      .zipWithUniqueId // ((bs, lon, lat), cellID))
      .cache
    
    // save bs-celldi map for later use
    cellID.map(t => "%d,%s,%s,%s".format(t._2, t._1._1, t._1._2, t._1._3))
      .saveAsTextFile(output + bsMapOut)

    val shortCellID = cellID.map { case ((bs, lon, lat), cellID) =>
      (bs, cellID)
    }
    
    // generate user movement history
    val movement = inputRDD.map { tuple => {
      val imsi = tuple(DataSchema.IMSI)
      val ttime = tuple(DataSchema.TTime).toDouble
      val bs = tuple(DataSchema.BS)
      (imsi, ttime, bs)
    }}.sortBy(_._2)  // sort by time

    // compress movement history: (imsi, time, baseStation)
    val cleaned = new TidyMovement().tidy(movement)

    // smash user identities by regenerating ids and add location lon/lat
    cleaned.keyBy(_._1)
      
      // replace with smashed user ids
      .join(userID).values
      .map { case ((imsi, time, bs), (userID)) => (bs, (time, userID)) }
      
      // replace with smashed cell ids
      .join(shortCellID).values
      .map { case ((time, userID), (cellID)) => (userID, time, cellID)}
      
      // order individual logs in time
      .sortBy(tuple => (tuple._1, tuple._2))
      .map { tuple => "%d,%.03f,%d".format(tuple._1, tuple._2, tuple._3) }
      .saveAsTextFile(output)

    // stop spark engine
    spark.stop()
    
  }

}