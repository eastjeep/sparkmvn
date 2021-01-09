package com.taikang.SparkSql

import com.taikang.SparkSql.DataFrameDataTypeDemo.mapToDataframe2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object RddDfDsConv {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName( "" )
      .master( "local[*]" )
      .getOrCreate()
    import  spark.implicits._
    // 测试的rdd数据

    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD( List( ("jayChou", 41), ("burukeyou", 23) ) )

    /*1、RDD*/
    /*无论之前的DataFrame是如何转换过来的, DataFrame转成成的RDD的类型都是Row.
     因为DataFrame = Dataset[Row], 所以实际是 Dataset[Row] 转成 RDD 返回的类型当然是Row*/
    // 1.1 与DataSet的互相转换
      // 1-  RDD[User] ===> DataSet
      // 带具体类型(User)的RDD内部可通过反射直接转换成 DataSet
    val dsUser: Dataset[User]  = rdd.map(t=>User(t._1, t._2)).toDS()
      // 2- DataSet ===> RDD[User]
    val rdduser: RDD[User] = dsUser.rdd
    //1.2 与DataFrame 的互相转换
    // ================ RDD转成DataFrame ===============================
    // 法1: 不带对象类型的RDD通过指定Df的字段转换
    //  RDD[(String, Int)] ===> DataFrame
    val userDF01: DataFrame =  rdd.toDF("name","age")
    //法2: 带对象类型的RDD直接转换
    // RDD[User] ====> DataFrame
    val userDF02: DataFrame = rdd.map(e => User(e._1,e._2)).toDF
    // ================DataFrame 转成 RDD ===============================
    val rddUser: RDD[Row]  = userDF01.rdd
    val userRdd02: RDD[Row]  = userDF02.rdd

    /*2、DataFrame*/
    //2.1、与DataSet 互相转换
    // 测试的df
    val df: DataFrame = rdd.toDF("name","age")
      // 1- DataFrame ===> DataSet
    val userDS: Dataset[User] = df.as[User]
    userDS.show(false)
      // 2-DataSet[User] ===> DataFrame
    val userDF: DataFrame = userDS.toDF
    userDF.show(false)

    /*3. 另一种，通过定义StructType来将RDD转换为DataFrame*/
    // 1- RDD[(String, Int)] --》	DataFrame
    var scDF: DataFrame = spark.createDataFrame(rdd)
    // 2- RDD[Row] ===> DataFrame
    val schema = StructType(Array(
      StructField("name",StringType,nullable = true),
      StructField("age",IntegerType,nullable = true)
    ))
    var scDF_Schema: DataFrame = spark.createDataFrame(scDF.rdd, schema)
  }
}
//做RDD转换为DF时，如果用样例类case class,不能将其定义到转换的同一个方法中
case class User(name: String, age: Int)
