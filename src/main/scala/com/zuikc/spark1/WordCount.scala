package com.zuikc.spark1

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("wordCount")
    //2.创建spark context，该对象是spark App的入口
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action算子
    sc.textFile(args(0)).flatMap(_.split("\\t")).
      map((_,1)).reduceByKey(_+_,1).
      sortBy(_._2,false).saveAsTextFile(args(1))
    //停止sc，结束该任务
    sc.stop()

  }
}


