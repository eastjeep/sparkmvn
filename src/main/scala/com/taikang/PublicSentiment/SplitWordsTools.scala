package com.taikang.PublicSentiment

import org.apache.log4j.{Level, Logger}
import com.taikang.PublicSentiment.SparkInitial._
import java.nio.file.Paths
import java.io.{FileWriter, PrintWriter}
import java.util
import java.util.Properties

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
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
//import org.apache.spark.ml.linalg.Vectors

// ----RDD-based
//import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.{Matrix,Matrices}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.Statistics
//scala 矩阵计算breeze
import breeze.linalg.{DenseMatrix, Transpose}

//standford nlp
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.ling.CoreAnnotations

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer

object SplitWordsTools {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // 中文分词 csv-dataframe
  def splitJieba_csv():Unit={
    val path = Paths.get("C:\\Users\\enint\\Downloads\\词库转TXT打包\\社会主义革命和社会主义建设词汇.txt")
    WordDictionary.getInstance().loadUserDict(path)
    // 将源数据提取为 RDD[String]
    spark.udf.register("combineSentence",(str:String) => combineSentence(str))
    val documents:DataFrame=spark.read.option("header","true").option("multiLine", "true")//.option("ignoreTrailingWhiteSpace","true")
     .csv("file:///C:\\Users\\enint\\Downloads\\chinese_news4.csv")
     .filter(length($"content") > 0)
     .limit(10000)
    //documents.createOrReplaceTempView("doc")
    //spark.sql("select count(1) from doc where content is not null and content != ''").show()

    // -- 分词content
    val id_word :DataFrame= documents.selectExpr("date","tag","headline","combineSentence(content)")
      .map(row =>(row.getString(0),row.getString(1),row.getString(2),new JiebaSegmenter().sentenceProcess(row.getString(3)):mutable.Buffer[String]))
      .toDF("date","tag","headline","wordVec")
      .withColumn("id",monotonically_increasing_id)

    //-- 去掉停止词
    val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")
    val removedDF = remover.transform(id_word).select("id","date","tag","headline","wordVec","removed").persist()

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

    // -- 分词headline
    val id_headline :DataFrame= documents.selectExpr("date","tag","headline","combineSentence(headline)")
      .map(row =>(row.getString(0),row.getString(1),row.getString(2),new JiebaSegmenter().sentenceProcess(row.getString(3)):mutable.Buffer[String]))
      .toDF("date","tag","headline","wordVec")
      .withColumn("id",monotonically_increasing_id)
    //-- 去掉停止词
    val removedDF_headline = remover.transform(id_headline).select("id","date","tag","headline","wordVec","removed").persist()
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
    hot_word.coalesce(1).write.mode(SaveMode.Overwrite).json("file:///c:\\ccc\\")

    // 计算 content的 hashing TF
    val hashingTF = new HashingTF()
      .setInputCol("removed").setOutputCol("HashFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(removedDF)

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
    val idf = new IDF()
      .setInputCol("HashFeatures")
      .setOutputCol("TF-IDF")
    val idfModel = idf.fit(featurizedData)
    val TFIDFResult = idfModel.transform(featurizedData)
    TFIDFResult.show(false)
    //TFIDFResult.printSchema()
    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    /*    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
        val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)*/

    // 训练word2Vec模型
    val word2Vec = new Word2Vec()
      .setInputCol("removed")
      .setOutputCol("word2VecFeatures")
      .setVectorSize(100)
      .setMinCount(0)
    val model = word2Vec.fit(removedDF)
    val result = model.transform(removedDF).persist()
    result.show(false)
    // result.printSchema()
    // -- 通过word2Vec 计算文档的相似度
    var feature:RDD[Vector] = result
      .rdd.map(row=>{
      org.apache.spark.mllib.linalg.Vectors.fromML(row.get(6).asInstanceOf[org.apache.spark.ml.linalg.Vector]) // word2Vec 取6； tf-idf 取7
     })
    feature.cache()

    // 训练 kmeans-model
    val kmeansTransformer = new KMeans().setK(3).setFeaturesCol("word2VecFeatures").setPredictionCol("prediction")
    val kmeansModel=kmeansTransformer.fit(result)
    val kmenas_result=kmeansModel.transform(result)
    kmenas_result.select("id","headline","prediction").show(false)

    //按列向量计算的相似度，但我们的文本向量是用横向量表示的
    /*   val correlMatrix:Matrix = Statistics.corr(feature,"spearman")//这里选用斯皮尔曼相关系数，皮尔逊系数输入"pearson"
       println(correlMatrix)
       System.out.println(correlMatrix.numCols)*/
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
    println(corMatrix)
    System.out.println(corMatrix.cols)

    // TextRank 计算重要性
    val sent=removedDF.select("id","removed").rdd.map(x=>(x(0) -> x(1)))
    //sent.keys.collect().foreach(println)
    val ranks=Array.ofDim[Double](corMatrix.cols.toInt).map(str=>1.0)  //构造1个数组，长度为相似度矩阵的长度，都赋值为1
    val res:Map[Int, Double]= sent.keys.collect().map{line =>
      val score=HotNewsTextRank.callTextRank(corMatrix,ranks,line.asInstanceOf[Long].toInt)
      line.asInstanceOf[Long].toInt -> score
    }.toMap
    //res.foreach(str=>println("句子"+str._1+"=="+str._2))
    val news_importance_rank=res.map(x=>(x._1,x._2)).toSeq.toDF("id","score")
    val news_importance_res=news_importance_rank.join(removedDF,"id")
      .select("id","headline","score").orderBy(desc("score"))
    // 将新闻重要性的结果写入json
      news_importance_res.coalesce(1).write.mode(SaveMode.Overwrite).json("file:///c:\\bbb\\")

    // 舆情趋势预测
    kmenas_result.select("")
  }

  //合并Excel单元格多行语句，去空白
  def combineSentence(document:String): String ={
      document.filterNot( (x: Char) => x.isWhitespace).trim.replaceAll( "\n", "\\#" )
  }

  // 中文分词 RDD
  def splitJieba1():Unit={
    // 将源数据提取为 RDD[String]
    val documents=sc.textFile("file:///C:\\zhangdong\\3工作\\taikang\\yuqing\\data\\*.txt")
    //分词
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
    // 计算权重 TF-IDF
   // val hashingTF = new HashingTF()
    //val featurizedData = hashingTF.transform(parseData1)
   // System.out.println(featurizedData.take(2))
 //  val tf: RDD[Vector] = hashingTF.transform(parseData)
  //  tf.take(5).foreach(println)
    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
   /* tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)*/

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
/*    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)*/
  }

  // 中文分词 dataframe
  def splitJieba2():Unit={
    // --将源数据提取为DataFrame
    val documents=sc.textFile("file:///C:\\zhangdong\\3工作\\taikang\\yuqing\\THUCNews_datasample\\*.txt")
                    .toDF("document")
                    .withColumn("id",monotonically_increasing_id)
    // -- 分词
    val id_word :DataFrame= documents.select("id","document")
      .map(row =>(row.getLong(0),new JiebaSegmenter().sentenceProcess(row.getString(1)):mutable.Buffer[String]))
      .toDF("id","wordVec")
    id_word.show(false)
    //var cc= id_word.select("wordVec").rdd.map(x=>x.mkString( ",").split(",")).foreach(println)
    //System.out.println(id_word.select("wordVec").collect().apply(1))
    /* id_word.printSchema()
     System.out.println(id_word.where(array_contains(id_word("wordVec"),"中国")).count())
     System.out.println(id_word.where(array_contains(id_word("wordVec"),"女排")).count())*/

    //-- 去掉停止词
    val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")
    val removedDF = remover.transform(id_word)
    removedDF.show(false)
    //-- 词频统计
    val wordData= removedDF.select("removed")
      .rdd.map(_.toString)
      //.filter(line=>line.contains("商瑞华"))
      .map(x=>x.substring(14,x.length-2))
      .flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map(word=>word.stripPrefix(" "))
    //wordData.toDF("wordData").show(false)
    val dataFreStatic=wordData.map(word=>(word,1)).reduceByKey(_+_)
      .map(x => (x._2, x._1)).sortByKey(false).map(x=> (x._2, x._1))

    // 计算分词权重  TF-IDF
   val hashingTF = new HashingTF()
      .setInputCol("removed").setOutputCol("HashFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(removedDF)
    featurizedData.select("HashFeatures").show(false)
    //训练TF-IDF模型
    val idf = new IDF()
      .setInputCol("HashFeatures")
      .setOutputCol("TF-IDF")
    val idfModel = idf.fit(featurizedData)
    val TFIDFResult = idfModel.transform(featurizedData)
    TFIDFResult.select("TF-IDF").show(false)
    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    /*    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
        val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)*/
  }

  // 英文分词 dataframe，k-means聚类
  /*def splitJieba3():Unit={
    // --将源数据提取为DataFrame
    val documents=sc.textFile("file:///C:\\zhangdong\\3工作\\taikang\\yuqing\\MIND_data\\MINDsmall_train\\news.tsv")
      .map(_.toString).map(_.split("\\\t")).map(x=>(x(0),x(1),x(2),x(3),x(4)))
      .toDF("newsid","type","keywords","title","document")
      .withColumn("id",monotonically_increasing_id)
    // -- 分词
    val id_word :DataFrame= documents.select("id","newsid","type","keywords","title","document")
      .map(row =>(row.getLong(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
                  getOriginalText(row.getString(5)):mutable.Buffer[String]))
      .toDF("id","newsid","type","keywords","title","wordVec")
      .limit(150)

    //-- 去掉停止词
    val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsEN.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("wordVec").setOutputCol("removed")
    val removedDF = remover.transform(id_word)
    removedDF.select("wordVec","removed").show(false)

    //-- 词频统计
      val wordData= removedDF.select("removed")
         .rdd.map(_.toString)
         .map(x=>x.substring(14,x.length-2))
         .flatMap(_.split(","))
         .filter(_.nonEmpty)
         .map(word=>word.stripPrefix(" "))//.persist(StorageLevel.MEMORY_AND_DISK)
         //wordData.toDF("wordData").show(false)
       val dataFreStatic=wordData.map(word=>(word,1)).reduceByKey(_+_)
         .map(x => (x._2, x._1)).sortByKey(false).map(x=> (x._2, x._1))
       dataFreStatic.take(100).foreach(println)

    // 计算分词权重  TF-IDF
/*    val hashingTF = new HashingTF()
      .setInputCol("removed").setOutputCol("HashFeatures").setNumFeatures(3000)
    val featurizedData = hashingTF.transform(removedDF)
    featurizedData.select("HashFeatures").show(false)
    //训练TF-IDF模型
    val idf = new IDF()
      .setInputCol("HashFeatures")
      .setOutputCol("TF-IDF")
    val idfModel = idf.fit(featurizedData)
    val TFIDFResult = idfModel.transform(featurizedData)*/

    // 计算分词权重  word2vec
    val word2Vec = new Word2Vec()
      .setInputCol("removed")
      .setOutputCol("word2VecFeatures")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(removedDF)
    val result = model.transform(removedDF)
    //result.show(false)

    // kmeans-model
    val kmeansTransformer = new KMeans().setK(3).setFeaturesCol("word2VecFeatures").setPredictionCol("prediction")
    val kmeansModel=kmeansTransformer.fit(result)
    val kmenas_result=kmeansModel.transform(result)
    kmenas_result.show(false)

  }*/

  // 英文分词工具方法
 /* def getOriginalText(text:String):util.ArrayList[String]={
    val wordList = new util.ArrayList[String]
    val properties = new Properties()
    //分词、分句、词性标注和次元信息。
    properties.put("annotators","tokenize,ssplit,pos,lemma,ner")
    val pipeline = new StanfordCoreNLP(properties)
    val document = new Annotation(text)
    pipeline.annotate(document)
    val words = document.get(classOf[CoreAnnotations.SentencesAnnotation])

    for (wordTemp <- words){
      for (token <- wordTemp.get(classOf[CoreAnnotations.TokensAnnotation])) {
        val originalWord: String = token.get(classOf[CoreAnnotations.LemmaAnnotation]) // 获取对应上面word的词元信息，即我所需要的词形还原后的单词
        wordList.add(originalWord)
      }
    }
    wordList
  }*/

def main(args: Array[String]): Unit = {
    splitJieba_csv()

   /*   //英文分词工具方法测试
    val str = "2 running Quick brown-foxes leap over lazy dogs in the summer evening. https://blog.csdn.net/LS7011846/article/details/101151185"
    val wordList = getOriginalText(str)
    import scala.collection.JavaConversions._
    for (word <- wordList) {
      System.out.println(word)
    }*/
  }
}
