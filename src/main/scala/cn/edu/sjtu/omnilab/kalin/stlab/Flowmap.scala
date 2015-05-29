package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable

case class MapEdge(interval: Integer, FROM: String, TO: String, NumUnique: Long, NumTotal: Long)

object Flowmap extends Serializable{

  def draw(input: RDD[STPoint], interval: Integer = 3600): RDD[MapEdge] = {

    // add interval to each log entry
    input.map(p => (p, p.time.toInt/interval))
      // group logs by user + interval
      .groupBy(m => (m._1.uid, m._2))
      .flatMapValues { case points: Iterable[(STPoint, Integer)] => {
        val sorted = points.toArray.sortBy(_._1.time)
        var lastLoc: String = sorted(0)._1.loc
        var fromtos: mutable.LinkedList[(String, String)] = new mutable.LinkedList[(String, String)]

        if ( sorted.size > 1) {
          for ( point <- sorted.slice(1, sorted.length) ) {
            val p = point._1
            val curLoc = p.loc
            if (curLoc != lastLoc)
              fromtos = fromtos :+ (lastLoc, curLoc)
            lastLoc = curLoc
          }
        }

        fromtos
    }}.map { case ((uid, interval), (from, to)) => (uid, interval, from, to)}
    .groupBy(m => (m._2, m._3, m._4)).mapValues { edges => {
      val users = edges.map(_._1)
      val total = users.size
      val unique = users.toArray.distinct.size
      (total, unique)
    }}.map(m => MapEdge(m._1._1, m._1._2, m._1._3, m._2._1, m._2._2))
  }

}
