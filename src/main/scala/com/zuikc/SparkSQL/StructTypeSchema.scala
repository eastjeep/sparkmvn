package com.zuikc.SparkSQL

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

//通过StructType 创造一个临时表，这样就不需要case class样例类了，可类比SparkSchema.scala文件
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    //创建sparkConf 并设置App名称
    val conf = new SparkConf().setAppName("SQL-1")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(","))
    //通过StructType直接指定每个字段的Schema
    val shema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = lineRDD.map(x => Row(x(0).toInt,x(1).trim(),x(2).toInt))
    //将schema信息应用到rowRDD上，转换成DataFrame
    val personDataFrame = sqlContext.createDataFrame(rowRDD,shema)

    //注册成表
    personDataFrame.registerTempTable("t_person")
    //传入sql语句
    val df = sqlContext.sql("select * from t_person order by age desc limit 4")
    //将结果以JSON的方式存储到指定位置
    df.write.json(args(1))

    //停止SC
    sc.stop()
  }

}
