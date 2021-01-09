package com.taikang.SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlTimeFunc {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    val orders=Seq(
      MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("深圳","钻石会员","钻石会员3个月",70),
      MemberOrderInfo("深圳","钻石会员","钻石会员12个月",300),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员6个月",120),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员3个月",45),
      MemberOrderInfo("深圳","黄金会员","黄金会员12个月",180),
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("北京","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("北京","黄金会员","黄金会员3个月",45),
      MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("上海","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("上海","黄金会员","黄金会员3个月",45)
    )
    //把seq转换成DataFrame
    val memberDF:DataFrame =orders.toDF()

    memberDF.rdd.map(x=>x.toString()).map(x=>x.split(",")).map(x=>(x(0),x(1).substring(0,4))).toDF().show()

    //把DataFrame注册成临时表
    memberDF.registerTempTable("orderTempTable")

    spark.sql("select * from orderTempTable where hour('') < 22 limit 5").show(false)
  }

}
