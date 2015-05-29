package cn.edu.sjtu.omnilab.kalin.hz

import cn.edu.sjtu.omnilab.kalin.stlab.{MPoint, CleanseMob}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

/**
 * Export Hangzhou mobile data in the long format.
 */
object TidyMovementJob {
  
  def main(args: Array[String]) {

    if (args.length != 2){
      println("Usage: TidyMovementJob <LOGSET> <LFDATA>")
      sys.exit(0)
    }

    val input = args(0)
    val output = args(1)

    // configure spark
    val conf = new SparkConf()
    conf.setAppName("Tidy HZ movement data")
    val spark = new SparkContext(conf)

    // read logs from data warehouse
    val inputRDD = spark.textFile(input)
      .map(_.split("\t"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // unique user IDs
    val userID = inputRDD
      .map( tuple => imsiPatch(tuple(DataSchema.IMSI)) )
      .distinct
      .zipWithUniqueId // (imsi, userID) 
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // save imsi-userid map for later use
    userID.map(t => "%d,%s".format(t._2, t._1))
      .saveAsTextFile(output + ".umap")

    // geographic location of base stations
    val cellID = inputRDD
      .map { parts =>
      val bs = parts(DataSchema.BS)
      val lon = parts(DataSchema.LON)
      val lat = parts(DataSchema.LAT)
      (bs, lon, lat)
    }.distinct
      .zipWithUniqueId // ((bs, lon, lat), cellID))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // save bs-celldi map for later use
    cellID.map(t => "%d,%s,%s,%s".format(t._2, t._1._1, t._1._2, t._1._3))
      .saveAsTextFile(output + ".bsmap")

    val shortCellID = cellID.map { case ((bs, lon, lat), cellID) => (bs, cellID) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // generate user movement history
    val movement = inputRDD.map { tuple => {
      val imsi = imsiPatch(tuple(DataSchema.IMSI))
      val ttime = tuple(DataSchema.TTime).toDouble
      val bs = tuple(DataSchema.BS)
      MPoint(uid=imsi, time=ttime, location=bs)
    }}.sortBy(_.time)

    // compress movement history: (imsi, time, baseStation)
    val cleaned = CleanseMob.cleanse(movement, minDays=14*0.75, tzOffset=8)
    // smash user identities by regenerating ids and add location lon/lat
    cleaned.keyBy(_.uid)
      // replace with smashed user ids
      .join(userID).values
      .map { case (mp, (userID)) => (mp.location, (mp.time, userID)) }
      // replace with smashed cell ids
      .join(shortCellID).values
      .map { case ((time, userID), (cellID)) => (userID, time, cellID)}
      // order individual logs in time
      .sortBy(tuple => (tuple._1, tuple._2))
      .map { tuple => "%d,%.03f,%d".format(tuple._1, tuple._2, tuple._3) }
      .saveAsTextFile(output)

    spark.stop()
  }

  /**
   * Cleanse IMSI with tail junk data
   */
  def imsiPatch(imsi: String): String = {
    if ( imsi.contains(','))
      return imsi.split(',')(0)
    return imsi
  }
}