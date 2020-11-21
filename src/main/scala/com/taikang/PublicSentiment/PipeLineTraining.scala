package com.taikang.PublicSentiment
import org.apache.log4j.{Level, Logger}
import com.taikang.PublicSentiment.SparkInitial._
import java.nio.file.Paths
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import spark.implicits._

//jieba分词
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

// -----dataframe-based
import org.apache.spark.ml.feature.{HashingTF, Tokenizer,IDF}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer, Word2Vec}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
//import org.apache.spark.ml.linalg.Vectors

// ----RDD-based
//import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.{Matrix,Matrices}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.Statistics
//scala 矩阵计算breeze
import breeze.linalg.{DenseMatrix, Transpose}

// pipeline
import org.apache.spark.ml.Pipeline

//standford nlp
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.ling.CoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable

object PipeLineTraining {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // 中文分词 csv-dataframe
  def main(args: Array[String]): Unit = {

    // 将源数据提取为 RDD[String]
    spark.udf.register("combineSentence",(str:String) => combineSentence(str))
    val documents:DataFrame=spark.read.option("header","true").option("multiLine", "true")
      .csv("file:///C:\\Users\\enint\\Downloads\\chinese_news4.csv")
      .filter(length($"content") > 0)

    // -- 分词content
    val id_word :DataFrame= documents.selectExpr("date","tag","headline","combineSentence(content)")
      .map(row =>(row.getString(0),row.getString(1),row.getString(2),new JiebaSegmenter().sentenceProcess(row.getString(3)):mutable.Buffer[String]))
      .toDF("date","tag","headline","wordVec")
      .withColumn("id",monotonically_increasing_id)
      .select("id","date","tag","headline","wordVec")

    // 建立pipeline各部分的模型
    //-- 去掉停止词
    val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")

    //-- 计算 content的 hashing TF
    val hashingTF = new HashingTF()
      .setInputCol("removed").setOutputCol("HashFeatures").setNumFeatures(2000)
    // 计算 contVector
    /*   val rawData=removedDF.select("id","removed")
       val model2: CountVectorizerModel = new CountVectorizer()
         .setInputCol("removed")
         .setOutputCol("CountVectorizerFeatures")
         .fit(rawData)
       val arr = model2.vocabulary
       val len = arr.length
       val arr1 = arr.indices.toArray
       val ind = arr.zip(arr1).toMap
       val indx = arr1.zip(arr).toMap

       val wordIndMap = sc.broadcast(ind)
       val wordIndMapx = sc.broadcast(indx)
       import spark.implicits._  //.toString.substring(14,x.length-2)
       val rddf = rawData.rdd.
         map(x => (x.get(1).toString,{
         var map : Map[Int, Double] = Map[Int, Double]()
         x.getAs[Seq[String]](1).foreach( t =>
         {
           val key = wordIndMap.value.getOrElse(t,-1)
           map.get(key) match {
             case Some(word) =>
                map += (key -> (word+1.0))
             case None =>  //不包含
                map += (key -> 1.0)
           }
         }
         )
         map
       }
       )).map(x => (x._1,{
         val t = x._2.toArray.sorted.unzip
         Vectors.sparse(len,t._1,t._2)
       })).toDF("user_log_acct","rawFeatures")
       rddf.show(false)*/
    //训练TF-IDF模型
    val idf = new IDF().setInputCol("HashFeatures").setOutputCol("TF-IDF")

    // 训练word2Vec模型
    val word2Vec = new Word2Vec()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("word2VecFeatures")
      .setVectorSize(100)
      .setMinCount(0)

    // 训练 kmeans-model
    val kmeansTransformer = new KMeans().setK(3).setFeaturesCol("word2VecFeatures").setPredictionCol("prediction")

    // 通过 pipeline模型fit训练数据
    val pipeline = new Pipeline()
      .setStages(Array(remover, word2Vec,kmeansTransformer))
    val model=pipeline.fit(id_word)

    // 保存训练好的model
    model.write.overwrite().save("file:///c:\\dddmodel\\")
  }

  //合并Excel单元格多行语句，去空白
  def combineSentence(document:String): String ={
    document.filterNot( (x: Char) => x.isWhitespace).trim.replaceAll( "\n", "\\#" )
  }

}
