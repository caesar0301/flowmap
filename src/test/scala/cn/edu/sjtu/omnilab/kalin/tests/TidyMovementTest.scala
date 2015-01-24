package cn.edu.sjtu.omnilab.kalin.tests

import cn.edu.sjtu.omnilab.kalin.hz.TidyMovement
import org.apache.spark.SparkContext.rddToPairRDDFunctions

class TidyMovementTest extends SparkJobSpec {

  "Spark Correlation implementations" should {
    
    val testFile = this.getClass.getResource("/hzlogs.txt").getPath()

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
      val cleaned = new TidyMovement().tidy(inputRDD)
      cleaned.count() must_== 9
    }

  }

}
