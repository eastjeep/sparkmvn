package com.taikang.PublicSentiment
import org.apache.log4j.{Level, Logger}
import com.taikang.PublicSentiment.SparkInitial._
import java.nio.file.Paths
import java.io.{FileWriter, PrintWriter}
import java.util
import java.util.Properties

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}
import org.apache.spark.storage.StorageLevel
import spark.implicits._
// -----dataframe-based
import org.apache.spark.ml.feature.{HashingTF, Tokenizer,IDF}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer, Word2Vec}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.ml.linalg.{Vector,Vectors}

object TrendAnalysis {

  def tendAnalysis(data_predict:DataFrame,document:DataFrame)={
    val raw_data=sc.textFile("file:///C:\\Users\\enint\\Downloads\\linear.txt")
    val map_data=raw_data.map{x=>
      val split_list=x.split("\t")
      (split_list(0).toDouble,split_list(1).toDouble,split_list(2).toDouble,split_list(3).toDouble,split_list(4).toDouble,split_list(5).toDouble,split_list(6).toDouble)
    }
    val df=spark.createDataFrame(map_data)
    val data = df.toDF("day1", "day2", "day3", "day4", "day5", "daydiff", "score")
    val colArray = Array("day1","day2","day3","day4","day5","daydiff")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(data)

    // 建立模型，预测谋杀率Murder
    // 设置线性回归参数
    val lr1 = new LinearRegression()
    val lr2 = lr1.setFeaturesCol("features").setLabelCol("score").setFitIntercept(true)
    // RegParam：正则化
    val lr3=lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.3)
    val lr=lr3
    // 将训练集合代入模型进行训练
    val lrModel = lr.fit(vecDF)

    // 预测集合
    val colArray_predict = Array("day1","day2","day3","day4","day5","daydiff")
    val assembler_predict = new VectorAssembler().setInputCols(colArray_predict).setOutputCol("features")
    val vecDF_predict: DataFrame = assembler_predict.transform(data_predict)

    // 预测计算
    val predictions: DataFrame = lrModel.transform(vecDF_predict)
    predictions.createOrReplaceTempView("pre")
 /*   val res=spark.sql("select id,concat('[',max(day1),',',max(day2),',',max(day3),',',max(day4),',',max(day5),']') realScores,"+
      "concat('[',concat_ws(',',collect_list(prediction)),']') predictScores from pre group by id")*/
 val pred= predictions.groupBy(col("id"),array("day1","day2","day3","day4","day5"))
      .agg(collect_list("prediction")).toDF("id","realScores","predictedScores")
      .join(document,"id")
      .select("headline","realScores","predictedScores")
      //.coalesce(10)
      //.write.mode(SaveMode.Overwrite).json("file:///c:\\ddd\\et.json")
     for (i <- 0 until 10){
      var t=pred.filter($"id".equalTo(i))
      var j=i+1
      var str:String="c:\\ddd\\"+j+s"_pred.json"
      var logger=new PrintWriter(new FileWriter(str))
       logger.println(t.toJSON.collect.mkString("[",",","]"))
       logger.close()
    }
/*    val tq=pred.filter($"id".equalTo(2))
   val logger=new PrintWriter(new FileWriter(s"c:\\ddd\\trend.json"))
    logger.println(tq.toJSON.collect.mkString("[",",","]"))
    logger.close()*/
  }

  def main(args: Array[String]): Unit = {
  }

}
