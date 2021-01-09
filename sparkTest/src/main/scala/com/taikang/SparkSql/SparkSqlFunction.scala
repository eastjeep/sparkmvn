package com.taikang.SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

//练习 rollup 与 cube
object SparkSqlFunction {
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

    //把DataFrame注册成临时表
    memberDF.registerTempTable("orderTempTable")
    //grouping sets
    spark.sql("select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType, product").show()
    spark.sql("select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType,product grouping sets ((area,memberType),(area,product))").show()

    //roll up
    spark.sql("select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType,product with rollup").show()
    //cube
    spark.sql("select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType,product with cube").show()
  }
}
case class MemberOrderInfo(area:String,memberType:String,product:String,price:Int)
