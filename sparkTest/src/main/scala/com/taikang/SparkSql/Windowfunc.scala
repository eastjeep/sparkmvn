package com.taikang.SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Windowfunc {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    /*val orders=Seq(
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
    )*/
    val orders=Seq(
      MemberOrder("深圳","钻石会员","钻石会员1个月","2020-09-01 12:20:12"),
      MemberOrder("深圳","钻石会员","钻石会员1个月","2020-09-02 12:22:12"),
      MemberOrder("深圳","黄金会员","黄金会员1个月","2020-09-01 12:20:12"),
      MemberOrder("深圳","黄金会员","黄金会员3个月","2020-09-01 15:20:12"),
      MemberOrder("北京","钻石会员","钻石会员1个月","2020-09-02 12:20:12.0"),
      MemberOrder("北京","钻石会员","钻石会员1个月","2020-09-03 12:20:12.0"),
      MemberOrder("北京","铂金会员","铂金会员3个月","2020-09-03 12:20:12.0"),
      MemberOrder("北京","黄金会员","黄金会员3个月","2020-09-03 12:20:12.0"),
      MemberOrder("北京","黄金会员","黄金会员3个月","2020-09-03 12:20:12.0"),
      MemberOrder("北京","黄金会员","黄金会员3个月","2020-09-03 12:20:13.0"),
      MemberOrder("上海","钻石会员","钻石会员1个月","2020-09-01 12:20:12"),
      MemberOrder("上海","钻石会员","钻石会员1个月","2020-09-01 12:20:12"),
      MemberOrder("上海","铂金会员","铂金会员3个月","2020-09-01 12:20:12"),
      MemberOrder("上海","黄金会员","黄金会员3个月","2020-09-01 12:20:12")
    )
    //把seq转换成DataFrame
    val memberDF:DataFrame =orders.toDF("area","memberType","product","price")
    //把DataFrame注册成临时表
    memberDF.createOrReplaceTempView("orderTempTable")
    //grouping sets
    //spark.sql("select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType, product").show()
    // row_number ,rank(),dense_rank()
    spark.sql("select *,row_number() over (partition by area order by price) rank from orderTempTable").show(false)

  }
}
case class MemberOrder(area:String,memberType:String,product:String,Datetime:String)