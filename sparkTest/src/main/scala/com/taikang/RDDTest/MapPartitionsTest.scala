package com.taikang.RDDTest

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MapPartitionsTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(Array(("a", 1, 11), ("a", 2, 12), ("b", 2, 13), ("b", 4, 14), ("c", 2, 15), ("e", 2, 16)), 4)
    //rdd1.foreach(println)
 /*   rdd1.foreach(line=>{
      var name=line._1
      var num=line._2.toString
      var newnum=line._3.toString
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
        "root", "123")
      val sql = s"insert into sparkmaptest(name,num,newnum) values('${name}','${num}','${newnum}') on duplicate key update newnum=values(newnum)"
      val statement= connection.createStatement()
      statement.executeUpdate(sql)
      statement.close()
      connection.close()
    }

    )*/

    rdd1.foreachPartition(x=>{
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
        "root", "123")
      val statement= connection.createStatement()
      x.foreach(line=>{
        var name=line._1
        var num=line._2.toString
        var newnum=line._3.toString
        val sql = s"insert into sparkmaptest(name,num,newnum) values('${name}','${num}','${newnum}') on duplicate key update newnum=values(newnum)"
        statement.executeUpdate(sql)
      })
      statement.close()
      connection.close()
    }

    )

   /* rdd1.mapPartitions(x=>{
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
        "root", "123")
      //val sql = "insert into syslog(action, event, host, insertTime, userName, update_Time) values(?,?,?,?,?,?)"
      val statement= connection.createStatement()
      var result=x.map(line=>{
          var name=line._1
          var num=line._2.toString
          val sql=s"insert into sparkmaptest(name,num) values('${name}','${num}')"
          statement.execute(sql)
      })
      statement.close()
      connection.close()
      result
    })*/
  }
}
