package cn.edu.sjtu.omnilab.flowmap.tests

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification._

abstract class SparkJobSpec extends SpecificationWithJUnit with BeforeAfterExample {

  @transient var sc: SparkContext = _

  def beforeAll = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
  }

  def afterAll = {
    if (sc != null) {
      sc.stop()
      sc = null
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

  def before = {}

  def after = {}

  override def map(fs: => Fragments) = Step(beforeAll) ^ super.map(fs) ^ Step(afterAll)

}

