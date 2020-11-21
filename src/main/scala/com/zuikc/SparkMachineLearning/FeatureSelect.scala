package com.zuikc.SparkMachineLearning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{ChiSqSelector, CountVectorizer, CountVectorizerModel, HashingTF, IDF, IndexToString, OneHotEncoder, PCA, PolynomialExpansion, RegexTokenizer, StringIndexer, Tokenizer, VectorIndexer, Word2Vec}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
//import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD

object FeatureSelect {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf()并设置App名称,设置master的地址
    val conf = new SparkConf().setAppName("Regression").setMaster("local[*]")
    //2. 创建spark context，该对象是spark App的入口
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local")
      .getOrCreate()
   val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    //取单个值
    sentenceData.select("label").show(1)
/*
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.take(3).foreach(println)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.take(3).foreach(println)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label","features").take(3).foreach(println)*/

    //Word2Vec
    // Input data: Each row is a bag of words from a sentence or document.
/*    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }*/

    //************************************************
    //CountVectorizerModel
/*    val df = spark.createDataFrame(Seq(
      (0, Array("a", "bb", "d","d","e","c")),
      (1, Array("a", "b", "bb", "e", "a","c"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(4)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c","d","e"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)*/

    //************************************
/*    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)*/

    //****************************************
    ////PCA
 /*   val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    println(data(1)(2))
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    df.take(3).foreach(println)

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)*/

    //多项式扩展*****************************************
/*    val data = Array(
      Vectors.dense(-2.0, 2.3),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.6, -1.1)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)
    val polyDF = polynomialExpansion.transform(df)
    polyDF.select("polyFeatures").take(3).foreach(println)*/


    //StringIndexer
  /*  val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("skip")    // 防止下面的测试案例出错，因为训练stringindexer没有d

    val test = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"))
    ).toDF("id", "category")
    val indexed = indexer.fit(df).transform(test)
    indexed.show()*/

    //**********************************************
    //IndexToString
 /*   val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show()*/

    //**********************************************
    //onehot Encoder
/*    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
     encoded.select("id", "categoryVec").show()*/

    //**********************************************
     // VectorIndexer
    val data1 = Seq(
      Vectors.dense(2, 5, 7, 3),
      Vectors.dense(4, 2, 4, 7),
      Vectors.dense(5, 3, 4, 7),
      Vectors.dense(6, 2, 4, 7),
      Vectors.dense(7, 2, 4, 7),
      Vectors.dense(8, 2, 5, 1))

     val data = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("features")

      val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()

    //*******************************************************
    //*******************************************************
    //卡方特征选择
    /* val data = Seq(
       (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
       (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
       (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
     )

     val df = spark.createDataFrame(data).toDF("id", "features", "clicked")

     val selector = new ChiSqSelector()
       .setNumTopFeatures(2)
       .setFeaturesCol("features")
       .setLabelCol("clicked")
       .setOutputCol("selectedFeatures")

     val result = selector.fit(df).transform(df)
     println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
     result.show()*/


/*    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      (0.0, Vectors.sparse(692, Array(45, 175, 500), Array(-1.0, 1.5, 1.3))),
      (1.0, Vectors.sparse(692, Array(100, 200, 300), Array(-1.0, 1.5, 1.3))))).toDF("label", "features")

    test.show(false)*/

/*    var data = Seq((0, Vectors.sparse(6, Array(0, 1, 2), Array(1.0, 1.0, 1.0))),
    (1, Vectors.sparse(6, Array(2, 3, 4), Array(1.0, 1.0, 1.0))),
    (2, Vectors.sparse(6, Array(0, 2, 4), Array(1.0, 1.0, 1.0))))
    var  df = spark.createDataFrame(data).toDF("id","features")
    df.take(3).foreach(println)*/

    //******************************************************************************************************
    ////*****************************************************************************************************
    // 练习RDD-based API
    // an RDD of local vectors
/*    val rows:RDD[Vector] = sc.makeRDD(Seq(Vectors.sparse(6, Array(0, 1, 2), Array(1.0, 1.0, 1.0))
                            ))
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)
    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println(m,n)

    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println(sm)*/

  }

}






