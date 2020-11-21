package com.taikang.PublicSentiment.practiceTestforRef

import com.taikang.PublicSentiment.SparkInitial.sc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object MLlibRDDBasedTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def tfIDF():Unit={
    // Load documents (one per line).
    val documents: RDD[Seq[String]] = sc.textFile("file:///C:\\zhangdong\\3工作\\taikang\\yuqing\\spark\\data\\mllib\\kmeans_data.txt")
      .map(_.split(" ").toSeq)
    documents.foreach(println)
    System.out.println(documents.take(2).apply(1))
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.foreach(println)
    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.foreach(println)
    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
  }

  def main(args: Array[String]): Unit = {
    tfIDF()
  }
}
