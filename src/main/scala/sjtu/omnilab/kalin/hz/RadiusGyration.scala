package sjtu.omnilab.kalin.hz

import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkContext, SparkConf}
import sjtu.omnilab.kalin.stlab.{RadiusGyration, GeoPoint, GeoMidpoint}

/**
 * Calculate radius of gyration of individuals
 */
object RadiusGyration {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length != 2){
      println("Usage: RadiusGyration <input> <output>")
      sys.exit(-1)
    }

    // configure spark
    val input = args(0)
    val output = args(1)
    val conf = new SparkConf()
      .setAppName("Generate properties of user movement")
    val spark = new SparkContext(conf)

    // read logs from repository
    val logs = spark.textFile(input).map(_.split("\t")).cache()

    // calculate user-specific RG
    val geoPoints = logs.map { line =>
      (line(3), GeoPoint(line(26).toDouble, line(25).toDouble))}
    val rg = (new RadiusGyration).radiusGyration(geoPoints)
    rg.map(x => x.productIterator.mkString("\t")).saveAsTextFile(output)

    spark.stop()
  }
}