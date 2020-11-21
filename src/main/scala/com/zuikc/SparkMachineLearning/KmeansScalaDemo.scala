package com.zuikc.SparkMachineLearning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeansScalaDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf()并设置App名称,设置master的地址
    val conf = new SparkConf().setAppName("Regression").setMaster("local[*]")
    //2. 创建spark context，该对象是spark App的入口
    val sc = new SparkContext(conf)
    //3.创建用于训练数据的RDD
    val dataRDD = sc.textFile("C:\\Users\\enint\\Desktop\\kmeans.txt")
    val trainRDD = dataRDD.map(lines => Vectors.dense(lines.split(" ").map(_.toDouble)))
    trainRDD.foreach(println)
    
  }
}
