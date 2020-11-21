package com.zuikc.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object NetWorkUpStateWordCount {
    Logger.getLogger("org").setLevel(Level.ERROR)

  val upDateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum+it._3.getOrElse(0)).map(m=>(it._1,m)))
    //模式匹配 类型匹配
    //hello,5
    iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(m=>(x,m))}
  }
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("NetWorkUpStateWordCount").setMaster("local[2]")
      val sc = new SparkContext(conf)
      // 设置DStream批次 时间间隔 为5秒
      val ssc = new StreamingContext(sc,Seconds(5))
      //设置checkpoint
      ssc.checkpoint("/opt/aaa")
      //通过网络将数据拉取过来
      val lines = ssc.socketTextStream("192.168.78.102",9999)
      //分词及统计(输入传输数据时，以空格分开)
      val words = lines.flatMap(_.split(" ")).map(x => (x,1)).updateStateByKey(upDateFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
      //传一个partion，并且还需要记住上一次的结果，所以为true

      //打印到控制台
      words.print()
      //开始计算
      ssc.start()
      //等待停止
      ssc.awaitTermination()
    }

}



/*
/opt/modules/spark-2.1.3/bin/spark-submit --class com.zuikc.SparkStreaming.NetWorkUpStateWordCount --master spark://hadoop-01:7077 --total-executor-cores 2 --executor-memory 512m spark-mvn-1.0-SNAPSHOT.jar
* */
