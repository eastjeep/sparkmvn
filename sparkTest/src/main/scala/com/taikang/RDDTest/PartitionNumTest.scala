package com.taikang.RDDTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, TaskContext}

//测试
object PartitionNumTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(Array(("a", 1, 11), ("bb", 2, 12), ("ee", 2, 16),("b", 2, 13), ("dd", 4, 14), ("c", 2, 15), ("e", 2, 16)))
    System.out.println(rdd1.getNumPartitions)
    val rdd2=spark.sparkContext.parallelize(Array((1,1), (5,10), (5,9), (2,4), (3,5), (3,6),(4,7), (4,8),(2,3), (1,2)))
    val pairRDD= rdd2.partitionBy(new HashPartitioner(3))

    //分区后的输出
    //printRDDPart(rdd1)
    printRDDPart(pairRDD)
    System.out.println("========>>><<<<")
    rdd1.foreachPartition(e=>{
      e.foreach(println)
      var id=TaskContext.getPartitionId()
      System.out.println(id)
    })
  }

  def mapPartIndexFunc(i1:Int,iter: Iterator[(Int,Int)]):Iterator[(Int,(Int,Int))]={
    var res = List[(Int,(Int,Int))]()
    while(iter.hasNext){
      var next = iter.next()
      res=res.::(i1,next)
    }
    res.iterator
  }
  def printRDDPart(rdd:RDD[(Int,Int)]): Unit ={
    var mapPartIndexRDDs = rdd.mapPartitionsWithIndex(mapPartIndexFunc)
    mapPartIndexRDDs.foreach(println( _))
  }
}


