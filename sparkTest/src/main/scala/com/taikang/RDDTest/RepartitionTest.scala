package com.taikang.RDDTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object RepartitionTest {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    //val rdd = spark.sparkContext.parallelize(1 to 10,100)
    //val repartitionRDD=rdd.repartition(4)
    val seq = List(("American Person", List("Tom", "Jim")), ("China Person", List("LiLei", "HanMeiMei")), ("Color Type", List("Red", "Blue")))
    val rdd1 = spark.sparkContext.parallelize(seq,2)
    println(rdd1.partitioner)
    println(rdd1.partitions.size)
    val rdd3=rdd1.partitionBy(new HashPartitioner(4))
    println(rdd3.partitions.size)
    println(rdd3.partitioner)
    val rdd2 = spark.sparkContext.makeRDD(seq)
    println(rdd2.partitioner)
    println(rdd2.partitions.size)

    // joinTEst
    val links=  spark.sparkContext.parallelize(Array(('A',Array('D')),('B',Array('A')),('C',Array('A','B')),('D',Array('A','C'))),3)
      .map(x=>(x._1,x._2)).cache()
    var ranks=spark.sparkContext.parallelize(Array(('A',1.0),('B',1.0),('C',1.0),('D',1.0)),2)
    val contribs=links.join(ranks,2).flatMap{
      case (url,(links,rank))=>links.map(dest=>(dest,rank/links.size))
    }
    contribs.foreach(println)

    //
    var start=(1 to 5).toList
    for(item <- start){
      println(item)
    }
  }
}
