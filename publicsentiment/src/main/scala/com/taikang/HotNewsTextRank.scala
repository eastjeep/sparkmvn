package com.taikang.PublicSentiment

import org.apache.spark.mllib.linalg.Matrix
//scala 矩阵计算breeze
import breeze.linalg._
import breeze.linalg.{DenseMatrix, Transpose}
//jieba分词
import com.huaban.analysis.jieba.JiebaSegmenter

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object HotNewsTextRank {
 def callTextRank(board:DenseMatrix[Double], ranks:Array[Double], num:Int):Double={
    val len=board.cols  //numRows
    val d = 0.85
    var added_score = 0.0
    for(j<- 0 until len){
      var fraction = 0.0
      var denominator:Double = 0.0
      // 先计算分子
      fraction= board(j,num) * ranks(j)
      // 计算分母
      for(k<-0 until len){
        denominator = denominator + board(j,k)
      }
      added_score += fraction / denominator
    }
    val weighted_score = (1 - d) + d * added_score
    return weighted_score
  }

  /**
    * 以下为测试 TextRank的算法
    * */
  def main(args: Array[String]): Unit = {

    val document="朝鲜外相今抵新加坡，穿黑西装打紫色领带，将与多国外长会谈。朝鲜外相李勇浩今抵新加坡。朝中社英文版2日的报道称，李勇浩率领朝鲜代表团于当天启程，除了新加坡，他还将访问伊朗。"
    val sent:Map[Int, List[String]]=splitSentence(document)
    sent.foreach(println)
    val metric=createMat(sent)                                  // 计算相似度矩阵 Array[Array[Double]]
    for(i<-metric)println(i.mkString(" "))                      // 打印相似度矩阵
    val ranks=Array.ofDim[Double](metric.length).map(str=>1.0)  //构造1个数组，长度为相似度矩阵的长度，都赋值为1
    //循环迭代结算得分
    for(i<-0 until 10) {
      val res: Map[Int, Double] = sent.keys.map { line =>
        val score = testTextRank( metric, ranks, line )
        line -> score
      }.toMap
      res.foreach( str => println( "句子" + str._1 + "==" + str._2 ) )
      //更新第一次循环后的得分
      res.map(str => ranks.update( str._1, str._2 ))
    }
  }
  def testTextRank(board:Array[Array[Double]],ranks:Array[Double],num:Int):Double={
    val len=board.length
    val d = 0.85
    var added_score = 0.0
    for(j<-0 until len){
      var fraction = 0.0
      var denominator:Double = 0.0
      // 先计算分子
      fraction= board(j)(num) * ranks(j)
      // 计算分母
      for(k<-0 until len){
        denominator = denominator + board(j)(k)
      }
      added_score += fraction / denominator
    }
    val weighted_score = (1 - d) + d * added_score
    return weighted_score
  }
  //中文分词
  def cutWord(sentense:String): List[String] ={
    val jieba = new JiebaSegmenter
    val sent_split:List[String]= jieba.sentenceProcess(sentense).asScala.toList.filterNot(str=>str.matches("，|“|”|：|、"))
    return sent_split
  }
  //拆分句子
  def splitSentence(document:String ): Map[Int, List[String]] ={
    val pattern="(。|！|？|\\.|\\!|\\?)".r
    val res=pattern.split(document).toList.zipWithIndex.map(str=>str._2->cutWord(str._1)).toMap
    return res
  }
  //构建句子间的相邻矩阵
  def createMat(document:Map[Int, List[String]]): Array[Array[Double]] ={
      val num = document.size
      // 初始化表
      val board=Array.ofDim[Double](num,num)
      for(i<-0 until  num){
        for(j<-0 until num){
          if(i!=j){
            board(i)(j) = similar_cal(document.get(i).get, document.get(j).get)._2
          }
        }
      }
      return board
    }
  //句子间的相似度计算：共同词个数/log（len1）+log(len2）
  def similar_cal(sent1:List[String],sent2:List[String]): ((String, String), Double) ={
      val same_word=sent1.intersect(sent2)
      val sim=same_word.size/(math.log(sent1.size)+math.log(sent2.size))
      (sent1.toString(),sent2.toString())->sim
    }

  }
