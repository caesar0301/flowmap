package cn.edu.sjtu.omnilab.kalin.tests

import java.util.Locale

import cn.edu.sjtu.omnilab.kalin.stlab.STUtils
import org.scalatest.{Matchers, FlatSpec}

class STUtilsTest extends FlatSpec with Matchers {

  it should "convert to UNIX time correctly" in {
    val ms = STUtils.ISOToUnix("2013-01-07 13:10:00") // local time
    println(ms)
  }

}
