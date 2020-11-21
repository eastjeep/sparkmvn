package com.zuikc.SparkMachineLearning

import org.apache.hadoop.mapred.Merger.Segment
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.dmg.pmml.regression.Term

object Demo2_MLR_News {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MLR_news").setMaster("local[*]")
    //2. 创建spark context，该对象是spark App的入口
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()

    val news = spark.read.format("CSV").option("header","true").load("C:\\Users\\enint\\Downloads\\peopleNews.csv")
    news.show(1,false)

    // 3. ETL
    val peopleWebNews = news.filter(news("title").isNotNull && news("created_time").isNotNull && news("label").isNotNull && news("content").isNotNull && news("source").isNotNull)
    println("过滤完整资讯条数为：" + peopleWebNews.count())
    println("各频道资讯条数为：")
    peopleWebNews.groupBy("label").count().show(false)

    //保留出现次数较多的label
    val peopleNews = peopleWebNews.filter(peopleWebNews("label").isin("国际","军事","财经","金融","时政","法制","社会"))

    //4, 将label列转换成索引，用StringIndexer，之后再用IndexToString把它转回去
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("label_num").fit(peopleNews)
    val indDF = indexer.transform(peopleNews)
    indDF.groupBy("label","label_num").count().show()

    //5.分词
    val tkz = new RegexTokenizer().setInputCol("content").setOutputCol("tokens").setPattern(",")
    val tkz_news  = tkz.transform(indDF)
    tkz_news.show(1,false)

    //6. 停用词
    val stopWords = spark.read.textFile("C:\\Users\\enint\\Downloads\\stopwordsCH.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("tokens").setOutputCol("removed")
    val removedDF = remover.transform(tkz_news)

    //7. 提取关键词频次特征
    val vectorizer = new CountVectorizer().setVocabSize(500).setMinDF(2).setInputCol("removed").setOutputCol("features").
      fit(removedDF)
    val vectorDF = vectorizer.transform(removedDF)
    vectorDF.select("label","features").show(1,false)

    // 8 MLR 训练与评估
    val Array(train,set) = vectorDF.randomSplit(Array(0.8,0.2),15L)
    train.persist()
    val lr = new LogisticRegression()
      .setMaxIter(40)
      .setRegParam(0.2)
      .setElasticNetParam(0.0)
      .setTol(1e-7)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(train)
    train.unpersist()
    //    打印逻辑回归的系数和截距
    println(s"Coefficients: ${lr.coefficientMatrix}")
    println(s"Intercept: ${lr.interceptVector} ")
    val trainingSummary = lr.summary
    //    获取每次迭代目标,每次迭代的损失,会逐渐减少
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    //9 训练集结果验证
    /*println("验证集各标签误报率(FPR):")
    trainingSummary.falsePositiveRateByLabel
      .zipWithIndex.foreach { case (rate, label) =>      println(s"label $label: $rate")
    }
    println("验证集各标签真分类率(TPR):")
    trainingSummary.truePositiveRateByLabel.zipWithIndex
      .foreach { case (rate, label) =>        println(s"label $label: $rate")
      }
    println("验证集各标签分类正确率:")
    trainingSummary.precisionByLabel.zipWithIndex
      .foreach { case (prec, label) =>        println(s"label $label: $prec")
      }
    println("验证集各标签召回率:")
    trainingSummary.recallByLabel.zipWithIndex.foreach {
      case (rec, label) =>        println(s"label $label: $rec")
    }
    println("验证集各标签F值:")
    trainingSummary.fMeasureByLabel.zipWithIndex.foreach    {
      case (f, label) =>      println(s"label $label: $f")
    }
    val accuracyLtr = trainingSummary.accuracy
    val falsePositiveRateLtr =      trainingSummary.weightedFalsePositiveRate
    val truePositiveRateLtr =      trainingSummary.weightedTruePositiveRate
    val fMeasureLtr = trainingSummary.weightedFMeasure
    val precisionLtr = trainingSummary.weightedPrecision
    val recallLtr = trainingSummary.weightedRecall*/
    /*println(s"分类准确率(Precision): $accuracyLtr\n误报率(FPR): $falsePositiveRateLtr\n真正类率(TPR): $truePositiveRateLtr\n" +
      s"F值(F-measure): $fMeasureLtr\n分类准确率(Precision): $precisionLtr \n召回率(Recall): $recallLtr")
*/
    // 10. 测试集评估

  }
}


