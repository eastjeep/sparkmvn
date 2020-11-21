package com.zuikc.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ScalaTransform {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf()并设置App名称,设置master的地址
    val conf = new SparkConf().setAppName("ScalaTransform").setMaster("local[*]")
    //2.创建spark context，该对象是spark App的入口
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 6)
/*    sc.parallelize(list, 3).mapPartitions(iterator => {
      val buffer = new ListBuffer[Int]
      while (iterator.hasNext) {
        buffer.append(iterator.next() * 100)
      }
      buffer.toIterator
    }).foreach(println)*/
    sc.parallelize(list, 4).mapPartitionsWithIndex((index, iterator) => {
      val buffer = new ListBuffer[String]
      while (iterator.hasNext) {
        buffer.append(index + "分区:" + iterator.next() * 100)
      }
      buffer.toIterator
    }).foreach(println)

    // intersection
    val rdd1 = sc.parallelize(List(1,2,3,4))
    val rdd2 = sc.parallelize(List(1,3,6,7))
    val rdd3= sc.makeRDD(List(1,4,7,10))
    rdd1.intersection(rdd2).foreach(println)

    //groupByKey
    val list1 = List(("hadoop", 2), ("spark", 3), ("spark", 5), ("storm", 6), ("hadoop", 2))
    sc.parallelize(list1,1).groupByKey().foreach(println)    // 这里的结果都是compactBuffer
    sc.parallelize(list1,2).groupByKey().map{t=>(t._1,t._2.toList)}.foreach(println)  //这里能还原为值

    //cogroup
    val list01 = List((1, "a"),(1, "a"), (2, "b"), (3, "e"))
    val list02 = List((1, "A"), (2, "B"), (3, "E"))
    //val list03 = List((1, "[ab]"), (2, "[bB]"), (3, "eE"),(3, "eE"))
    sc.parallelize(list01).cogroup(sc.parallelize(list02)).map(t=>(t._1,t._2.toString())).foreach(println)

    //aggregateByKey
    val list03 = List(("hadoop", 3), ("hadoop", 2), ("spark", 4), ("spark", 3), ("storm", 6), ("storm", 8))

    //flatmap
    val list04 = List(List(1, 2), List(3), List(), List(4, 5))
    sc.parallelize(list04).flatMap(_.toList).map(_ * 10).foreach(println)

    sc.stop()

  }
}
