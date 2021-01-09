package com.taikang.SparkSql

import com.alibaba.fastjson.JSONObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*使用 jsonObject拼装 嵌套Json*/
object SparkJsonDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
     val data=Seq(("1","aa"),("2","bb"),("3","cc")).toDF("id","name")
    data.show()

    val cols=data.columns.toList
    cols.foreach(println)
    data.rdd.mapPartitions(parts=>{  //data.toJson.rdd
      val qq=new JSONObject()
      var result = List[JSONObject]()
      parts.foreach(e=>{
          cols.foreach(col =>{
            qq.put(col,e.getAs(col))
          })
          var ww=qq.toJSONString
          qq.clear()
          qq.put("people",ww)
          var ss=qq.toJSONString
          qq.clear()
          qq.put("basic",ss)
          var res=qq
          qq.clear()
          result=res :: result
      })
      result.iterator
    }).foreach(println(_))

    val zhangsan = new  JSONObject()
      zhangsan.put( "name", "张三" )
      zhangsan.put( "age", 18.4 )
    println(zhangsan)


  }
}
