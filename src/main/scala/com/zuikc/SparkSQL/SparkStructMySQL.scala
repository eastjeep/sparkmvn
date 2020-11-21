package com.zuikc.SparkSQL

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

// 将spark查询结果写入mysql
object SparkStructMySQL {
  def main(args: Array[String]): Unit = {
    //创建sparkConf 并设置App名称
    val conf = new SparkConf().setAppName("Spark-Struct").setMaster("local[2]")
    // 创建sc对象，用于生成RDD
    val sc = new SparkContext(conf)
    //创建 SQLContext()对象
    val sqlContext =new SQLContext(sc)

    //创建RDD
    val lineRDD =sc.textFile("hdfs://hadoop-01:9000/inputSpark/person.txt").map(_.split(","))

    //通过StructType直接指定每个字段的Schema（将RDD转成DataFrame时，需要这个schama）
    val shema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )
    //关联RDD和case class  //如果是StructType,就要把lineRDD映射到rowRDD
    //val personRDD = lineRDD.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    val personRDD = lineRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    //将schema信息应用到rowRDD上，转换成DataFrame
    val personDataFrame = sqlContext.createDataFrame(personRDD,shema)

    //SQL风格---注册成表----     *********************************
    personDataFrame.registerTempTable("t_person")
    //传入sql语句
    val df  = sqlContext.sql("select * from t_person order by age desc limit 2")

    //将结果写入到Mysql
    df.write.format("jdbc")
      .option("url","jdbc:mysql://192.168.78.102:3306/mydb")
      .option("driver", "com.mysql.cj.jdbc.Driver") // 这里要加入mysql-connector-java依赖
      .option("dbtable","person1") //表名 //与hdfs输出有些像，不允许有这个表已存在 ；如果是已存在，可以加.mode("append"),增量导入
      .option("user","root")
      .option("password","Root@123")
      .save()
  }
}

//case class Person(id:Int,name:String,age:Int)