package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.spark.{SparkConf, SparkContext}

object GeoRangeJob {

   def main(args: Array[String]) = {

     if (args.length < 2){
       println("usage: GeoRangeJob <in> <out>")
       sys.exit(-1)
     }

     val input = args(0)
     val output = args(1)

     val conf = new SparkConf()
       .setAppName("Counting the geographic ranges")

     val spark = new SparkContext(conf)
     val inputRDD = spark.textFile(input).
     map(line => line.split("\t"))
     
     val lonRDD = inputRDD.map(_(DataSchema.LON).toDouble)
     .filter(v => v >= -180 && v <= 180)
     val latRDD = inputRDD.map(_(DataSchema.LAT).toDouble)
     .filter(v => v >= -90 && v <= 90)
     
     val range = Array(("LON:", lonRDD.min, lonRDD.max),
       ("LAT:", latRDD.min, latRDD.max))
     
     spark.parallelize(range).saveAsTextFile(output)
     
     spark.stop()
   }

 }
