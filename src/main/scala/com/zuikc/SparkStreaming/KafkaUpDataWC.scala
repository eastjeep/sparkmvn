package com.zuikc.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object KafkaUpDataWC {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置本地运行模式，local[2]代表开启两个线程
    val conf =new SparkConf().setMaster("local[2]").setAppName("KafkaUpDataWC")
    val sc = new SparkContext(conf)
    //设置DStream批次时间间隔为5秒
    val ssc=new StreamingContext(sc,Seconds(5))
    //设置checkpoint
    ssc.checkpoint("/opt/aaa")
    //通过网络将数据拉取过来，定义kafka参数
    var kafkaDStream: ReceiverInputDStream[(String,String)] =KafkaUtils.createStream(
       ssc,
      "192.168.78.102:2181",
      "test",
      Map("test"->1)
    )

    //分词 及 组建键值对
    val words = kafkaDStream.flatMap(t=>t._2.split(" ")).map(x=>(x,1)).updateStateByKey(upDateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //传一个partion，并且还需要记住上一次的结果，所以为true

    //打印到控制台
    words.print()
    //输出到HDFS
    words.saveAsTextFiles("hdfs://hadoop-01:9000/outputKafka")
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
  val upDateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum+it._3.getOrElse(0)).map(m=>(it._1,m)))
    //模式匹配 类型匹配
    //hello,5
    iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(m=>(x,m))}
  }
}

