package com.zuikc.SparkMachineLearning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RegressionTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
      //1.创建 SparkConf()并设置App名称,设置master的地址
      val conf = new SparkConf().setAppName("Regression").setMaster("local[*]")
      //2.创建spark context，该对象是spark App的入口
      val sc = new SparkContext(conf)
      //3.创建用于训练数据的RDD
      val training = sc.makeRDD(Seq("0,1 2 3 1", "1,2 4 1 5", "0,7 8 3 6", "1,2 5 6 9").map(line => LabeledPoint.parse(line)))
           // *************************************************************
           // 读取hive中的数据
          val spark = SparkSession.builder().appName("hive")
          .enableHiveSupport()
          .getOrCreate()
          spark.sql("use mydb")
          val data = spark.sql("select * from air_condition_model_dwd" +
              "where air_ID = args(0) and  people >= 1").toDF()
          val colArray2 = Array("temp_in","temp_out", "humid_in","air_quality")
          val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(data)
          val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.9, 0.1), seed = 12345)
          //模型训练
          val Model = new LogisticRegression().setLabelCol("switch").setPredictionCol("switch_predict").
                      setFeaturesCol("features").fit(trainingDF)
          val predictions = Model.transform(testDF)

          val predictionRdd = predictions.select("switch_predict","switch").rdd.map{
              case Row(switch_predict:Double,switch:Double)=>(switch_predict,switch)
          }
          val accuracy = new MulticlassMetrics(predictionRdd).accuracy
          Model.save("hdfs://hadoop-01:9000/xhl/data/test6")
          val sameModel = LogisticRegressionModel.load(spark.sparkContext,"hdfs://hadoop-01:9000/xhl/data/test6")
          sameModel.toPMML(sc,"hdfs://hadoop-01:9000/xhl/data/test7")
          sc.stop()
        //*******************************************************8

      //4.模型训练
      val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)
      val test = sc.makeRDD(Seq("0,1 2 3 1").map(line => LabeledPoint.parse(line)))

      // 5. 在测试集上计算原始得分
      val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(predictionAndLabels)
      //val accuracy = metrics.accuracy
      println(s"Accuracy = $accuracy")

      // Save and load model
      //    model.save(spark.sparkContext, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
      //    val sameModel = LogisticRegressionModel.load(spark.sparkContext,"target/tmp/scalaLogisticRegressionWithLBFGSModel")

      model.toPMML(sc, "hdfs://hadoop-01:9000/xhl/data/test5")

    }
}
