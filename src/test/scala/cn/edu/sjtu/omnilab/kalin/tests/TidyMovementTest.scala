package cn.edu.sjtu.omnilab.kalin.tests

import cn.edu.sjtu.omnilab.kalin.hz.{DataSchema, TidyMovement}
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

class TidyMovementTest extends SparkJobSpec {

  "Spark Correlation implementations" should {
    
    val testFile = this.getClass.getResource("/hzlogs.txt").getPath()
    
    def extractMov(inputRDD: RDD[String]): RDD[(String, Double, String)] = {
      val movement = inputRDD
        .map( line => {
          val tuple = line.split("\t")
          val imsi = tuple(DataSchema.IMSI)
          val time = tuple(DataSchema.TTime).toDouble
          val cell = tuple(DataSchema.BS)
          (imsi, time, cell)
      }).sortBy(_._2)
      
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
      count must_== 14
    }
    
    "case 2: return correct format of tidied data" in {
      val inputRDD = sc.textFile(testFile)
      val movement = extractMov(inputRDD)
      val cleaned = new TidyMovement().tidy(movement)
      cleaned.count() must_== 9
    }

    "case 3: parameter gap works normally in TidyMovement" in {
      val inputRDD = sc.textFile(testFile)
      val movement = extractMov(inputRDD)
      val cleaned = new TidyMovement().tidy(movement, 300)
      cleaned.count() must_== 6

    }

  }

}
