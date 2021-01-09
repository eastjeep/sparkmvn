package com.taikang.PublicSentiment

import java.util
import com.taikang.PublicSentiment.SparkInitial.sc
import org.apache.log4j.{Level, Logger}

object SplitUserLog {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def splitLog():Unit={
    // 将源数据提取为 RDD[String]
    val documents=sc.textFile("file:///C:\\zhangdong\\3工作\\taikang\\yuqing\\MIND_data\\MINDsmall_train\\behaviors.tsv")
    documents.take(5).foreach(println)
/*    //分词
    val parseData1= documents.map({ x =>
      var str = if (x.length > 0) {
        new JiebaSegmenter().sentenceProcess(x)
      }
      str.toString
    }) //.filter(line=>line.contains("商瑞华"))
      // 词频统计
      .map(x=>x.substring(1,x.length-1))
      .flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map(word=>word.stripPrefix(" "))
      .map(word=>(word,1)).reduceByKey(_+_)
      .map(x => (x._2, x._1)).sortByKey(false).map(x=> (x._2, x._1)).take(30)
    //.foreach(println)

    // 分词
    val parseData=documents.map { x =>
      var str = if (x.length > 0) {
        new JiebaSegmenter().sentenceProcess(x)
      }
      util.Arrays.asList(str.toString)
    }
    */
  }

  def main(args: Array[String]): Unit = {
    splitLog()
  }
}
