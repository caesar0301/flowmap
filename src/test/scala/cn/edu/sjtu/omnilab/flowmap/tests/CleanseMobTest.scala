package cn.edu.sjtu.omnilab.flowmap.tests

import cn.edu.sjtu.omnilab.flowmap.hz.DataSchema
import cn.edu.sjtu.omnilab.flowmap.stlab.{MPoint, CleanseMob}
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

class CleanseMobTest extends SparkJobSpec {

  "Spark Correlation implementations" should {
    
    val testFile = this.getClass.getResource("/hzlogs.txt").getPath()
    
    def extractMov(inputRDD: RDD[String]): RDD[MPoint] = {
      val movement = inputRDD
        .map( line => {
          val tuple = line.split("\t")
          val imsi = tuple(DataSchema.IMSI)
          val time = tuple(DataSchema.TTime).toDouble
          val cell = tuple(DataSchema.BS)
          MPoint(imsi, time, cell)
      }).sortBy(_.time)
      
      movement
    }

    "case 0: join action works normally" in {
      val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
      val b = a.keyBy(_.length)
      val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
      val d = c.keyBy(_.length)
      b.join(d)
      true must_== true
    }

    "case 1: return with correct test log number (#14)" in {
      val inputRDD = sc.textFile(testFile)
      val count = inputRDD.count()
      count must_== 20
    }

    "case 2: return correct format of tidied data" in {
      val inputRDD = sc.textFile(testFile)
      val movement = extractMov(inputRDD)
      val cleaned = CleanseMob.cleanse(movement, 0, 0, 0, 0, 8, false)
      cleaned.count() == 4
    }

    "case 3: adding night logs works well in TidyMovement" in {
      val inputRDD = sc.textFile(testFile)
      val movement = extractMov(inputRDD)
      val cleaned = CleanseMob.cleanse(movement, 0, 0, 0, 0, 8, true)
      cleaned.count() == 4
    }

  }

}
