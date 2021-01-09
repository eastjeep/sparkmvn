package com.taikang.RDDTest

import org.apache.spark.{SparkConf, SparkContext}

object ByKey {
  def main(args: Array[String]): Unit = {
      // groupByKey()
    //reduceByKey()
    sortByKey()
  }
  def groupByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
    val sc=new SparkContext(conf)
    val salary = Array(
      Tuple2("dept1", 10000),
      Tuple2("dept2", 11000),
      Tuple2("dept1", 13000),
      Tuple2("dept2", 12000)
    )
    val rdd1 = sc.parallelize(salary)
    val rdd2 = rdd1.groupByKey()
    rdd2.foreach(println)
    rdd2.sortByKey().foreach(t => {
      println(t._1)
      val salary1: Iterator[Int] = t._2.iterator
      while (salary1.hasNext){
        println(salary1.next())
      }
    })
  }

  def reduceByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val salary = Array(
      Tuple2("dept1", 10000),
      Tuple2("dept2", 11000),
      Tuple2("dept1", 13000),
      Tuple2("dept2", 12000)
    )
    val rdd1 = sc.parallelize(salary)
    val rdd2 = rdd1.reduceByKey(_+_)
    rdd2.foreach(x => {
      println("部门：" + x._1 + "的工资和：" + x._2)
    })
  }
  def sortByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val salary = Array(
      Tuple2(10000,"dept1"),
      Tuple2(11000,"dept2"),
      Tuple2(13000,"dept1"),
      Tuple2(12000,"dept2")
    )
    // Scala的链式编程写起来会比较简洁
    sc.parallelize(salary).sortByKey().foreach(println(_))
  }
}

