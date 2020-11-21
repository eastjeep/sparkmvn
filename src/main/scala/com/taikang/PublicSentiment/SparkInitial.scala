package com.taikang.PublicSentiment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkInitial {
  private var appName = "publicsentiment"
  //val conf = new SparkConf().setAppName(s"${this.appName}")//.setMaster("local[2]")
  //val sc = new SparkContext(conf)
  //val hiveContext = new HiveContext(sc)
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(s"${this.appName}")
    .master("local[*]")
    //.config("hive.metastore.uris", "thrift://192.168.78.131:9083")
    //.enableHiveSupport()
    .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext
/*  def setSparkAppName(appName: String): Unit = {
    this.appName = appName
  }*/
}

