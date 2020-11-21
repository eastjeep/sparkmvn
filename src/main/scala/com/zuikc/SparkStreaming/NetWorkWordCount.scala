package com.zuikc.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

// 通过向端口实时传数据，在这里监控该端口，把数据取过来
object NetWorkWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //  local[2]代表开启2个线程，*的话代表开启全部线程
    val conf = new SparkConf().setAppName("NetWorkWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //设置DStream批次 时间间隔 为5秒
    val ssc = new StreamingContext(sc,Seconds(5))
    //通过网络将数据拉取过来
    val lines = ssc.socketTextStream("192.168.78.102",9999)
    //分词及统计
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x=>(x,1))  // 或 words.map((_,1))
    // 统计出现次数
    val wordCount=pairs.reduceByKey(_+_)
    //打印到控制台
    wordCount.print()
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}
