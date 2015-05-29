package cn.edu.sjtu.omnilab.flowmap.hz

import org.apache.spark.{SparkContext, SparkConf}

object FilterDataGeo {
  
  val lonRange = (120.0, 120.5)
  val latRange = (30.0, 30.5)
  
  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: FilterDataGeo <in> <out>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    
    val conf = new SparkConf()
      .setAppName("Filter logs by geographic range")
    
    val spark = new SparkContext(conf)
    val inputRDD = spark.textFile(input)

    // filter out data outside given ranges
    val filtered = inputRDD.filter(line => {
      val parts = line.split("\t")
      val lon = parts(DataSchema.LON).toDouble
      val lat = parts(DataSchema.LAT).toDouble
      var selected = false
      if( lon >= lonRange._1 && lon <= lonRange._2 &&
      lat >= latRange._1 && lat <= latRange._2)
        selected = true
      selected
    })
    
    filtered.saveAsTextFile(output)
    
    spark.stop()
  }

}
