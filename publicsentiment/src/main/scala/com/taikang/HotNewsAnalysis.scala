package com.taikang.PublicSentiment

import java.io.{FileWriter, PrintWriter}

import com.taikang.PublicSentiment.SparkInitial.{sc, spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.desc
import spark.implicits._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import breeze.linalg.DenseMatrix

object HotNewsAnalysis {
  def hotNewsCal(documents:DataFrame)={
    // 通过pipeline计算word2Vec向量
    val result=PipeLinePro.analysisRealData(documents)

    // -- 通过word2Vec 计算文档的相似度矩阵
    var feature:RDD[Vector] = result
      .rdd.map(row=>{
      org.apache.spark.mllib.linalg.Vectors.fromML(row.get(6).asInstanceOf[org.apache.spark.ml.linalg.Vector]) // word2Vec 取6； tf-idf 取7
    })
    feature.cache()
    val dim=documents.count().toInt   // 数据文件中的新闻条数(文档数量)
    var corMatrix=DenseMatrix.zeros[Double](dim,dim)
    val board=Array.ofDim[Double](dim,dim)
    val r=feature.collect()
    for (i <- 0 until dim){
      for (j <- 0 until dim){
        if(i!=j){
          var r1=sc.parallelize(r.apply(i).toArray)
          var r2=sc.parallelize(r.apply(j).toArray)
          corMatrix(i,j)=Statistics.corr(r1,r2,"spearman")
        } else {
          corMatrix(i,j)=1
        }
      }
    }

    // TextRank 计算重要性
    val sent=result.select("id","removed").rdd.map(x=>(x(0) -> x(1)))
    //sent.keys.collect().foreach(println)
    val ranks=Array.ofDim[Double](corMatrix.cols.toInt).map(str=>1.0)  //构造1个数组，长度为相似度矩阵的长度，都赋值为1
    val res:Map[Int, Double]= sent.keys.collect().map{line =>
      val score=HotNewsTextRank.callTextRank(corMatrix,ranks,line.asInstanceOf[Long].toInt)
      line.asInstanceOf[Long].toInt -> score
    }.toMap
    //res.foreach(str=>println("句子"+str._1+"=="+str._2))
    val news_importance_rank=res.map(x=>(x._1,x._2)).toSeq.toDF("id","score")
    val news_importance_res=news_importance_rank.join(result,"id")
      .select("id","headline","score").orderBy(desc("score"))
    // 将新闻重要性的结果写入json
    //news_importance_res.coalesce(1).write.mode(SaveMode.Overwrite).json("file:///c:\\bbb\\")
    val logger=new PrintWriter(new FileWriter(s"c:\\ddd\\hotnews.json",true))
    logger.println(news_importance_res.toJSON.collect.mkString("[",",","]"))
    logger.close()
  }
}
