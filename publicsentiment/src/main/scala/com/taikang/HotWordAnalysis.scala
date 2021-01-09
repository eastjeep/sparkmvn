package com.taikang.PublicSentiment

import java.io.{FileWriter, PrintWriter}

import com.taikang.PublicSentiment.SparkInitial.spark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, desc}
import spark.implicits._

object HotWordAnalysis {
    def hotWordCal(documents:DataFrame)={
        // 计算获得分词-去除stopwords的数据集
        val removedDF=HotWordSplit.splitContent(documents)
        val removedDF_headline=HotWordSplit.splitHeadline(documents)

        //-- 词频统计content
        val wordData= removedDF.select("removed").rdd.map(_.toString).filter(_.nonEmpty)
          //.filter(line=>line.contains("商瑞华"))
          .map(x=>x.substring(14,x.length-2))
          .flatMap(_.split(","))
          .map(word=>word.stripPrefix(" "))
        val contentFreStatic=wordData.map(word=>(word,1)).reduceByKey(_+_)
          .map(x => (x._2, x._1)).sortByKey(false).map(x=> (x._2, x._1))
        val contentTotalNum:Double=contentFreStatic.values.collect().toList.sum
        //contentFreStatic.take(50).foreach(println)

        //-- 词频统计headline
        val wordHeadline= removedDF_headline.select("removed").rdd.map(_.toString).filter(_.nonEmpty)
          //.filter(line=>line.contains("商瑞华"))
          .map(x=>x.substring(14,x.length-2))
          .flatMap(_.split(","))
          .map(word=>word.stripPrefix(" "))
        val headFreStatic=wordHeadline.map(word=>(word,1)).reduceByKey(_+_)
          .map(x => (x._2, x._1)).sortByKey(false).map(x=> (x._2, x._1))
        val headTotalNum:Double=headFreStatic.values.collect().toList.sum

        //  热词计算
        val h=headFreStatic.map(x=>(x._1,(x._2.toDouble)*2*100 / headTotalNum)).toDF("word","num1")
        val c=contentFreStatic.map(x=>(x._1,(x._2.toDouble)*100 / contentTotalNum)).toDF("word","num2")
        val hot_word=h.join(c,"word").withColumn("score",col("num1")+col("num2"))
          .orderBy(desc("score")).select("word","score").limit(50)
       // hot_word.coalesce(1).write.mode(SaveMode.Overwrite).json("file:///c:\\ccc\\")

       val logger=new PrintWriter(new FileWriter(s"c:\\ddd\\hotword.json",true))
        logger.println(hot_word.toJSON.collect.mkString("[",",","]"))
        logger.close()
    }
}
