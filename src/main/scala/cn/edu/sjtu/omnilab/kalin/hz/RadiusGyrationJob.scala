package cn.edu.sjtu.omnilab.kalin.hz

import cn.edu.sjtu.omnilab.kalin.stlab.{GeoPoint, RadiusGyration}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Calculate radius of gyration of individuals
 */
object RadiusGyrationJob {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    
    if (args.length != 2){
      println("Usage: RadiusGyration <input> <output>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)

    // configure spark
    val conf = new SparkConf()
      .setAppName("Generate properties of user movement")
    val spark = new SparkContext(conf)

    // read logs from repository
    val logs = spark.textFile(input).map(_.split("\t")).cache()

    // calculate user-specific RG
    val geoPoints = logs.map { line =>
      (line(DataSchema.IMSI),
        GeoPoint(line(DataSchema.LAT).toDouble,
          line(DataSchema.LON).toDouble))}
    val rg = (new RadiusGyration).radiusGyration(geoPoints)
    
    rg.map(x => x.
      productIterator.mkString("\t")).
      saveAsTextFile(output)

    spark.stop()
  }
}
