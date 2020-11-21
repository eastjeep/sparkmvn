package com.taikang.PublicSentiment

import com.huaban.analysis.jieba.JiebaSegmenter
import com.taikang.PublicSentiment.SparkInitial.spark
import com.taikang.PublicSentiment.SplitWordsTools.combineSentence
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

import scala.collection.mutable
import scala.collection.JavaConversions._

object HotWordSplit {
    def splitContent(documents:DataFrame):DataFrame={
      // 注册spark UDF用于处理分词中的换行符、空白字符
      spark.udf.register("combineSentence",(str:String) => SplitWordsTools.combineSentence(str))
      // -- 分词content
      val id_word :DataFrame= documents.selectExpr("id","date","tag","headline","combineSentence(content)")
        .map(row =>(row.getLong(0),row.getString(1),row.getString(2),row.getString(3),new JiebaSegmenter().sentenceProcess(row.getString(4)):mutable.Buffer[String]))
        .toDF("id","date","tag","headline","wordVec")
      //  .withColumn("id",monotonically_increasing_id)

      //-- 去掉停止词
      val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
      val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")
      val removedDF = remover.transform(id_word).select("id","date","tag","headline","wordVec","removed").persist()
      return  removedDF
    }

    def splitHeadline(documents:DataFrame):DataFrame={
      // 注册spark UDF用于处理分词中的换行符、空白字符
      spark.udf.register("combineSentence",(str:String) => SplitWordsTools.combineSentence(str))
      // -- 分词headline
      val id_headline :DataFrame= documents.selectExpr("id","date","tag","headline","combineSentence(headline)")
        .map(row =>(row.getLong(0),row.getString(1),row.getString(2),row.getString(3),new JiebaSegmenter().sentenceProcess(row.getString(4)):mutable.Buffer[String]))
        .toDF("id","date","tag","headline","wordVec")
       // .withColumn("id",monotonically_increasing_id)
      //-- 去掉停止词
      val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
      val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")
      val removedDF_headline = remover.transform(id_headline).select("id","date","tag","headline","wordVec","removed").persist()
      return  removedDF_headline
    }
}
