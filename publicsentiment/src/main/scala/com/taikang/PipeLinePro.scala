package com.taikang.PublicSentiment

import com.huaban.analysis.jieba.JiebaSegmenter
import com.taikang.PublicSentiment.PipeLineTraining.combineSentence
import com.taikang.PublicSentiment.SparkInitial.spark
import com.taikang.PublicSentiment.SplitWordsTools.combineSentence
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{length, monotonically_increasing_id}
import spark.implicits._

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * 通过加载model，获得每篇新闻的word2Vec向量，用于后续计算热点新闻
  * */
object PipeLinePro {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def analysisRealData(documents:DataFrame):DataFrame={
    // 注册spark UDF用于处理分词中的换行符、空白字符
    spark.udf.register("combineSentence",(str:String) => SplitWordsTools.combineSentence(str))
    // -- 分词content
    val id_word_real :DataFrame= documents.selectExpr("id","date","tag","headline","combineSentence(content)")
      .map(row =>(row.getLong(0),row.getString(1),row.getString(2),row.getString(3),new JiebaSegmenter().sentenceProcess(row.getString(4)):mutable.Buffer[String]))
      .toDF("id","date","tag","headline","wordVec")
     // .withColumn("id",monotonically_increasing_id)
      .select("id","date","tag","headline","wordVec")

    //加载训练好的model
    val model = PipelineModel.load("file:///c:\\dddmodel\\")
    //该model可认为是一个transformer
    val result=model.transform(id_word_real)
    //result.select("id","headline","prediction","removed","word2VecFeatures").show(false)
    result
  }

  def main(args: Array[String]): Unit = {
    // 加载真实的新的数据源
    spark.udf.register("combineSentence",(str:String) => SplitWordsTools.combineSentence(str))
    val documents:DataFrame=spark.read.option("header","true").option("multiLine", "true")
          .csv("file:///C:\\Users\\enint\\Downloads\\chinese_news4.csv")
          .filter(length($"content") > 0)
          .limit(20)
    analysisRealData(documents)
  }
}
