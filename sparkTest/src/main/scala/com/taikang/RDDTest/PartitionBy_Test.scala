package com.taikang.RDDTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

object PartitionBy_Test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(Array(("a", 1), ("a", 2), ("b", 1), ("b", 3), ("c", 1), ("e", 1)), 4)

    val rdd=rdd1.partitionBy(new RangePartitioner(2,rdd1,ascending = true,samplePointsPerPartitionHint = 20))
    //val rdd=rdd1.partitionBy(new HashPartitioner(4))

    val result = rdd.mapPartitionsWithIndex {
      (partIdx, iter) => {
        val part_map = scala.collection.mutable.Map[String, List[(String, Int)]]()
        while (iter.hasNext) {
          val part_name = "part_" + partIdx
          var elem = iter.next()
          if (part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(String, Int)] {
              elem
            }
          }
        }
        part_map.iterator
      }
    }.collect
    result.foreach(x => println(x._1 + ":" + x._2.toString()))

  }
}
