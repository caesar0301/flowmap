package sjtu.omnilab.kalin.hz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Figure out the importance places for individuals.
 */
object ImportantPlaces {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length != 2){
      println("Usage: CountLogs <input> <output>")
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



    spark.stop()
  }
}
