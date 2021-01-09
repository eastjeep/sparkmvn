package com.taikang.SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/*集合数据类型array\map的取值方式*/
object DataframeArrayMapDataProcess {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark:SparkSession=SparkSession.builder().appName("")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val data = (1 to 5).map(b_gen)
    val df = spark.createDataFrame(data)
    df.show(false)
    df.printSchema()

    /*数组索引*/
    //c的数据类型为array，我们可以单纯使用点的方式把数组中的某个结构给提取出来
    //同样可以使用expr("c['a']")或col("c")("a")的方式获得相同的结果。
    df.select("c.a").show(10, false)
    df.select("c.a").printSchema
    //一个很有用的表达式explode，它能把数组中的元素展开成多行数据
    df.select(expr("explode(c.a)")).show
    df.select(expr("explode(c.a)")).printSchema
    df.select(expr("posexplode(c.a)")).show
    df.select(expr("explode(c)")).show
    df.select(expr("explode(c)")).printSchema
    ////inline也是一个非常有用的函数，它可以把array[struct[XXX]]直接展开成XXX
    df.select(expr("inline(c)")).show
    df.select(expr("inline(c)")).printSchema

    /*map索引*/
    /*1、点表达式 a.b
      2、中括号表达式 expr(“a[‘b’]”)
      3、小括号表达式 col(“a”)(“b”) */
    df.select(expr("posexplode(d)")).printSchema
    df.select(expr("posexplode(e)")).printSchema
    df.select(expr("posexplode(d)")).show
    df.select(expr("posexplode(f)")).show
    df.select(expr("posexplode(f)")).printSchema

    //我们可以使用点表达式去用map的key取value
    //如果key不存在这行数据会为null
    df.select("d.key_1").show  //key_1 是df的d字段列(map)的一个key,这个select就把key_1对应的值取出来
    df.select("d.key_1").printSchema
    //数字为key同样可以使用
    //对于数字来讲，expr("e[1]")、expr("e['1']")、col("e")(1)、col("e")("1")这四种表达式都可用
    //只是最后取得的列名不同
    df.select("e.1").show
    df.select("e.1").printSchema
    //对于df的f字段列，用struct作为map的key
    //这种情况下，我们可以用namedExpressionSeq表达式类构造这个struct
    df.select(expr("f[('str_2' AS a, 2 AS b)]")).show
    df.select(expr("f[('str_2' AS a, 2 AS b)]")).printSchema
  }
  def a_gen(i: Int) = A(s"str_$i", i)
  def b_gen(i: Int) = B(
    (1 to 5).map(a_gen).toList,
    (1 to 5).map(j => s"key_$j" -> a_gen(j)).toMap,
    (1 to 5).map(j => j -> s"value_$j").toMap,
    (1 to 5).map(j => a_gen(j) -> s"value_$j").toMap
  )
}
case class A(a: String, b: Int)
case class B(c: List[A], d: Map[String, A], e: Map[Int, String], f: Map[A, String])
