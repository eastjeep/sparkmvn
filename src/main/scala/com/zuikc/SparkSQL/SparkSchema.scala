package com.zuikc.SparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSchema {
  def main(args: Array[String]): Unit = {
    //创建sparkConf 并设置App名称
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
    // SQLContext 要依赖SparkContext该对象是spark App的入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //创建RDD
    //val lineRDD = sc.textFile(args(0)).map(_.split(","))   //在linux下跑jar包时用于输入参数
    val lineRDD = sc.textFile("hdfs://hadoop-01:9000/inputSpark/person.txt").map(_.split(","))
    //关联RDD和case class  //如果是StructType,就要把lineRDD映射到rowRDD
    val personRDD = lineRDD.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF()
    // 这里也可以直接关联RDD创建DataFrame，不用样例类
    //val personDF1 = sc.textFile(args(0)).map(_.split(",")).map(x=>(x(0).toInt,x(1),x(2).toInt)).toDF("id","name","Age")

    // DSL风格  ************************************************8
     personDF.select("name").show
     personDF.filter("age>20").show

    //SQL风格---注册成表----     *********************************
     personDF.registerTempTable("t_person")
    //传入sql语句
     val df  = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
    //df.write.json(args(1))         ////在linux下跑jar包时用于输入参数
    df.write.json("hdfs://hadoop-01:9000/output8")   //在run configurations设置成root用户，否则写不进去
    // 停止SparkContext
    sc.stop()
  }
}

case class Person(id:Int,name:String,age:Int)
