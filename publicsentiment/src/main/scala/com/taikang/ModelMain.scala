package com.taikang.PublicSentiment
import org.apache.log4j.{Level, Logger}
import com.taikang.PublicSentiment.SparkInitial.{sc, spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import spark.implicits._

/**
  * Analysis for public sentiment
  * 1. 热点新闻，热词
  * 2. 传播特征分析
  * 3. 发展趋势预测
  * By dongZhang, Date 20200929
  * */
object ModelMain {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    // 加载要分析的数据
    val documents:DataFrame=spark.read.option("header","true").option("multiLine", "true")//.option("ignoreTrailingWhiteSpace","true")
      .csv("file:///C:\\Users\\enint\\Downloads\\chinese_news4.csv")
      .filter(length($"content") > 0)
      .withColumn("id",monotonically_increasing_id)
      .limit(10)
    val data_predict:DataFrame=spark.read.option("header","true").option("multiLine", "true")//.option("ignoreTrailingWhiteSpace","true")
      .csv("file:///C:\\Users\\enint\\Downloads\\chinese_news5.csv")
      .select($"id",col("day1").cast(DoubleType), col("day2").cast(DoubleType)
        ,col("day3").cast(DoubleType),col("day4").cast(DoubleType),col("day5").cast(DoubleType)
        ,col("daydiff").cast(DoubleType))

/*      HotWordAnalysis.hotWordCal(documents)
      HotNewsAnalysis.hotNewsCal(documents)*/
      TrendAnalysis.tendAnalysis(data_predict,documents)
  }

}
