package com.taikang.SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable.ArrayBuffer

object DataFrameDataTypeDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
      val spark:SparkSession=SparkSession.builder().appName("")
        .master("local[*]")
        .getOrCreate()

    strToJsonArray(spark)
  }
  /*数组列转到多个列*/
  def onearrayToMultiCol(spark:SparkSession)={
    import spark.implicits._
    val arraydata=Seq(
      Row("zhang",List("java","scala","c++")),     //这里用Array与List都可以
      Row("li",List("spark","scala","c#")),
      Row("wang",List("python","R",""))
    )
    val arrayRDD=spark.sparkContext.parallelize(arraydata)
    //val arraySchema=new StructType().add("name",StringType).add("subjects",ArrayType(StringType))
    val arraySchema=StructType(Array(             //这里用Array与List都可以
      StructField("name",StringType,true),
      StructField("subjects",ArrayType(StringType),true)
    ))
    val arrayDF=spark.createDataFrame(arrayRDD,arraySchema)
    arrayDF.show(false)
    arrayDF.printSchema()
    arrayDF.withColumn("containValue",array_contains(col("subjects"),"java")).filter($"containValue"===true).show(false)
    //将list里的每个值展开为1个单独的列
    val res=arrayDF.select($"name"+: (0 until 3).map(i => $"subjects"(i).alias(s"language$i")): _*)
    res.show(false)
  }
  /*嵌套数组列转到多个列*/
  def embedarrayToMultiCol(spark:SparkSession)={
    import spark.implicits._
    val arraydata=Seq(
      Row("zhang",List(List("java","scala","c++"),List("java","scala"))),     //这里用Array与List都可以
      Row("li",List(List("spark","scala","c#"),List("spark","scala"))),
      Row("wang",List(List("python","R",""),List("python","R")))
    )
    val arrayRDD=spark.sparkContext.parallelize(arraydata)
    //val arraySchema=new StructType().add("name",StringType).add("subjects",ArrayType(StringType))
    val arraySchema=StructType(Array(             //这里用Array与List都可以
      StructField("name",StringType,true),
      StructField("subjects",ArrayType(ArrayType(StringType)),true)
    ))
    val arrayDF=spark.createDataFrame(arrayRDD,arraySchema)
    arrayDF.show(false)
    arrayDF.printSchema()
    //arrayDF.withColumn("containValue",array_contains(col("subjects"),"java")).filter($"containValue"===true).show(false)
    //将list里的每个值展开为1个单独的列
    val res=arrayDF.select($"name"+: (0 until 2).map(i => $"subjects"(i).alias(s"language$i")): _*)
    res.show(false)
  }
  /* 1列是list，转为多行*/
  def onearrayToMultiRow(spark:SparkSession)={
    import spark.implicits._
    val arraydata=Seq(
      Row("zhang",List("java","scala","c++")),     //这里用Array与List都可以
      Row("li",List("spark","scala","c#")),
      Row("wang",List("python","R",""))
    )
    val arrayRDD=spark.sparkContext.parallelize(arraydata)
    //val arraySchema=new StructType().add("name",StringType).add("subjects",ArrayType(StringType))
    val arraySchema=StructType(Array(             //这里用Array与List都可以
      StructField("name",StringType,true),
      StructField("subjects",ArrayType(StringType),true)
    ))
    val arrayDF=spark.createDataFrame(arrayRDD,arraySchema)
    arrayDF.show(false)
    arrayDF.printSchema()
    //将list里的每个值展开都放到1个字段下面
    val res=arrayDF.select($"name",explode($"subjects").alias("unpivot"))
    res.show(false)

  }
  /* 把多个列字段，转为1个列*/
  def multiColToOneCol(spark:SparkSession)={
    import spark.implicits._
    val data=Seq(("a",1,11),("b",2,22),("c",3,33)).toDF("id","num","age")
    data.show(false)
    data.createOrReplaceTempView("tt")
    val data_unpivot=spark.sql("select id,STACK(2,'num',num,'age',age) as (key,value) from tt")
    data_unpivot.show(false)
  }
  /*structType 取值*/
  def structType(spark:SparkSession)={
    import spark.implicits._
    val data=Seq(("a",1,11),("b",2,22),("c",3,33)).toDF("id","num","age")
    data.show(false)
    val data1=data.selectExpr("id","struct(num,age) as complex","num","age")
    data1.printSchema()
    data1.select(col("complex").getField("num").alias("new_num"),col("age")).show(false)
    data1.select($"complex.num").show(false)
  }
  /*arrayType 取值*/
  def arrayType(spark:SparkSession)={
    import spark.implicits._
    val data=Seq(("a","ee ff",11),("b","gg hh ii",22),("c","ii jj",33)).toDF("id","desc","age")
    data.show(false)
    data.select(split(col("desc")," ").alias("desc_array")).select(expr("desc_array[0]")).show(false)
    data.select(split(col("desc")," ").alias("desc_array")).select(col("desc_array").getItem(0)).show(false)
    data.select(split(col("desc")," ").alias("desc_array")).withColumn("numArray",size(col("desc_array"))).show(false)
  }
  /*mapType 取值*/
  def mapType(spark:SparkSession)={
    import spark.implicits._
    import org.apache.spark.sql.Row
    val data=Seq(("a",1,11),("b",2,22),("c",3,33)).toDF("id","num","age")
    val data1=data.select($"id",map(col("num"),col("age")).alias("complex_map"))
    data1.show(false)
    data1.toJSON.show(false)   //{"id":"a","complex_map":{"1":11}}
    data1.select(expr("complex_map['1']")).show()
    data1.select(expr("complex_map")).printSchema()
    //explode可以展开map类型
    data1.select($"complex_map",explode($"complex_map")).show(false)   // 将map展开为key，value两个字段列

    println("---->>>>> map_from_arrays将两列的list分别设为key和value组合成1列，里面是多个map")
    val data2=Seq(Row(List("1","a","2","b"),List("3","c","4","d")),Row(List("5","e","6","f"),List("7","g","8","h")))
    val arrayRDD=spark.sparkContext.parallelize(data2)
    val arraySchema=StructType(Array(             //这里用Array与List都可以
      StructField("arr1",ArrayType(StringType),true),
      StructField("arr2",ArrayType(StringType),true)
    ))
    spark.createDataFrame(arrayRDD,arraySchema).show(false)
    val data3=spark.createDataFrame(arrayRDD,arraySchema).select(map_from_arrays($"arr1",$"arr2")).show(false)

    /*将1个list/array字段列，合成map*/
    println("--------->>>>>>>>> 1个array字段转变为map")
    val data4=spark.createDataFrame(arrayRDD,arraySchema).select(array(slice($"arr1",1,2),slice($"arr1",3,2)).alias("newarr1"))
    data4.show(false)
    data4.printSchema()
    val data5=data4.select(expr("newarr1[0]").alias("aa"),expr("newarr1[1]").alias("bb"))
      .withColumn("items",map(col("aa").getItem(0),col("aa").getItem(1)))
    data5.show(false)
    data5.toJSON.show(false)

    println("--------->>>>>>>>> map_concat")
    val df=spark.sql("select map(1, 'a', 2, 'b') as amap,map(2, 'c', 3, 'd') as bmap")
    df.show(false)
    df.select(map_concat($"amap",$"bmap")).show(false)
    df.select(map_keys($"amap")).show(false)
    df.select(element_at($"amap",map_keys($"amap")(2))).show(false)  // 取对应mapkey的value值
  }
  /*将嵌套数组中的每一个数组变为1个map*/
  def arrayToMap(spark:SparkSession)={
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.Row
    // Sample data:
    val df = Seq(
      ("id1", "t1", Array(("n1", 4L), ("n2", 5L))),
      ("id2", "t2", Array(("n3", 6L), ("n4", 7L)))
    ).toDF("ID", "Time", "Items")
    df.show(false)
    // Create UDF converting array of (String, Long) structs to Map[String, Long]
    val arrayToMap = udf[Map[String, Long], Seq[Row]] {
      array => array.map { case Row(key: String, value: Long) => (key, value) }.toMap
    }
    // apply UDF
    val result = df.withColumn("Items", arrayToMap($"Items"))
    result.show(false)

    /*scala array转成map*/
    val arrdata=Array(Array(1,20),Array(3,5),Array(7,9))
    val  newarrmap=arrdata.map{
      case Array(x,y) => (x,y)
    }.toMap
    newarrmap.foreach(println)
  }
  /*listType 取值*/
  def listType(spark:SparkSession)={
    import spark.implicits._
    /*  val list=List("a","b","v","b").toDF("listvalue")
        list.show(false)
        val list1 = spark.sparkContext.parallelize(List(("a", "b", "c", "d"))).toDF()
        list1.show(false)
        val list2 = spark.sparkContext.parallelize(List(("a1","b1","c1","d1"),("a2","b2","c2","d2"))).toDF
        list2.show(false)*/

    /*  var data_csv = Seq(
          ("ke,sun"),
          ("tian,sun")
        ).toDF("CST_NO")
        data_csv.show(false)
        data_csv.printSchema()   // CST_NO: string (nullable = true)
        var neg_tmp = data_csv.select("CST_NO").collect().map(_(0)).toList
        neg_tmp.foreach(println)
        println(neg_tmp.length)   //2
        // 取第一行 neg_tmp(0)
        var neg_list = neg_tmp(0).toString.split(",")
        println(neg_list)*/

    /*将list变为1个字段*/
    var lst = List[String]("57.54", "trusfortMeans", null, "20190720", "5852.00", null, null, "25.77", null)
    var name_list = List("idm", "CO", "distrn","dayId", "Ant", "CLP", "CAC", "PE_num","CE")
    var df = List((lst.toArray)).toDF("features")
    df.show(false)
    /*将list变为1行*/
    // 注 表格里值一定要统一格式 ，全转化为String(null除外，没意义) 如果没有则toDF方法报错
    var data = List[String]("57.54", "trusfortMeans", null, "20190720", "5852.00", null, null, "25.77", null)
    var name_list2 = List("idm", "CO", "distrn","dayId", "Ant", "CLP", "CAC", "PE_num","CE")
    var df2 = List((data.toArray)).toDF("features")
    val elements = name_list2.toArray
    val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("features").getItem(idx).as(alias) }
    val res = df2.select(sqlExpr : _*)
    res.show(false)

  }
  /*jsonType 取值*/
  def jsonType(spark:SparkSession)={
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.Row
    val rowRDD  = spark.sparkContext.textFile(".\\src\\main\\resources\\file2").map( x => x.split("-")).map( x => Row(x(0),x(1).trim()))
    val schema = StructType(
      Seq(
        StructField("id",StringType,true)
        ,StructField("jsonString",StringType,true)
      )
    )
    val jsonDF=spark.createDataFrame(rowRDD,schema)
    jsonDF.show(false)
    jsonDF.select(get_json_object($"jsonString","$.myJSONKey")).show(false)
    jsonDF.select(get_json_object($"jsonString","$.myJSONKey.myJSONValue")).show(false)
    jsonDF.select(get_json_object($"jsonString","$.myJSONKey.myJSONValue[0]")).show(false)
    jsonDF.select(json_tuple($"jsonString","myJSONKey").alias("jsontuple")).show(false)  //，如果json字符串只有一个层级，可以使用该方法提取json对象

    /*from_json简单使用，会解析成一个Struct类型的列col（数据类型一样的话也可以是Array类型）
     可以查看col的Schema，所以可以根据col.*查询全部，也可以col.属性查询特定属性*/
    val df = Seq (
      (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cn": "United States", "timestamp" :1475600496 }"""),
      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cn": "Norway", "timestamp" :1475600498 }"""))
      .toDF("id","json_col")
    val mySchema = new StructType()
      .add("device_id",IntegerType)
      .add("device_type",StringType)
      .add("ip",StringType)
      .add("cn",StringType)
      .add("timestamp",TimestampType)
      //下面的from_json取到的是json串中的值 [0, sensor-ipad, 68.161.225.1, United States, 2016-10-05 01:01:36] col这1列拿到的是json串中的value，再对应着schama转换为json
    df.select(from_json($"json_col",mySchema) as "col").show(false)
    df.select(from_json($"json_col",mySchema) as "col").printSchema()
    df.select(from_json($"json_col",mySchema) as "col").select(expr("col.*")).show(false)
    /*to_json 是把一个包含StructType，ArrayType或MapType的列转换为具有指定模式（类型中推出）的JSON字符串列，
    所以要先把要转换的列封装成StructType，ArrayType或MapType格式 */
    val df1 = df.select(from_json($"json_col",mySchema) as "col").select($"col.*")
    df1.printSchema()
    // 如果是所有列的化，这样写struct($"*")
    df1.select(to_json(struct($"device_id",$"ip",$"timestamp")).alias("json_col")).show(false)

    /*json字符串对应的1个字段转为dataframe的多个字段*/
    val jsoncol=spark.sparkContext.textFile(".\\src\\main\\resources\\json").toDF("colstr")
    jsoncol.printSchema()
    val mySchema2 = new StructType()
      .add("Make",StringType)
      .add("Model",StringType)
      .add("RetailValue",StringType)
      .add("Stock",IntegerType)
    val jsondata=jsoncol.select(from_json($"colstr",mySchema2) as "coljsonstr").select($"coljsonstr.*")
    jsondata.show(false)
    jsondata.select(to_json(struct($"Make",$"Model",$"RetailValue",$"Stock")).alias("jsonstr")).show(false)
  }

  /*map转为dataframe*/
  def mapToDataframe(spark:SparkSession)={
    import scala.collection.mutable.ArrayBuffer
    import spark.implicits._
    val dt = Array(("1", "2", "3", "4", "5"), ("6", "7", "8", "9", "10"))
    val data = spark.createDataFrame(dt).toDF("col1", "col2", "col3", "col4", "col5")
    //dtypes返回一个string类型的二维数组，返回值是所有列的名字以及类型
    val col2Type = data.dtypes.toMap   //(col3,StringType) (col2,StringType)...
    col2Type.foreach(println)
    val colName_list = data.columns.toList
    println(colName_list)  //List(col1, col2, col3, col4, col5)
    var arrbuf = ArrayBuffer[(String,String)]()
    for (col <- colName_list){
      //println("col===="+col.toString)
      //println("Col2Type===="+Col2Type.get(col).get.toString)
      arrbuf += ((col.toString,col2Type.get(col).get.toString))
    }
    println(arrbuf)  //ArrayBuffer((col1,StringType), (col2,StringType), (col3,StringType), (col4,StringType), (col5,StringType))
    val df = spark.createDataFrame(arrbuf).toDF("colname", "type")
    df.show()

  }
  /*map转为dataframe*/
  def mapToDataframe2(spark:SparkSession)={
    import scala.collection.mutable.ArrayBuffer
    import spark.implicits._
    //3. scala的Map数据结构
    val map = Map("aa" -> "aaa", "bb" -> "bbb","cc"->"ccc")
    //4. map的所有key
    val mk = map.keys
    //5. 创建rdd
    val rdd = spark.sparkContext.parallelize(Seq(map))
    rdd.foreach(println)
    //6. 根据map的key取出所有的值，构建新的rdd，并转成dataFrame
    val frame = rdd.map(x => {
      val bb = new ArrayBuffer[String]()
      for (k: String <- mk) {
        bb.+=(x(k))
      }
      bb
    }).map(x => (x(0), x(1),x(2))).toDF("k1", "k2","k3")
    //7. 打印
    frame.show()
  }
  /*json字符串作为新的1列加入dataframe中，可以为普通字符串，或array*/
  def jsonStrAddToDataframeCol(spark:SparkSession)={
    import spark.implicits._
    val rowsRdd: RDD[Row] = spark.sparkContext.parallelize( Seq( Row("xiaoming", 30), Row("ling", 28) ) )
    val fields = Array(StructField("name", StringType, nullable = true), StructField("age", IntegerType, nullable = true))
    println(fields)
    val schema = StructType(fields)
    val peopleDataFrame = spark.createDataFrame(rowsRdd, schema)
    val udf_2 = udf((s: Any) => {
      """[{"k":"kkk","v":100,"animal_interpretation":{"is_large_animal":true,"is_mammal":false}}]"""
    })
    //定义json的Schema
    val arrayStruct = ArrayType(StructType(Seq(
      StructField("k", StringType, true), StructField("v", DoubleType, true),
      StructField("animal_interpretation", StructType(Seq(
        StructField("is_large_animal", BooleanType, true),
        StructField("is_mammal", BooleanType, true)
      )))
    )), true)
    //添加一列字符串的值
    val df1 = peopleDataFrame.withColumn("jsonstr", udf_2(col("age")))
    df1.printSchema()
    df1.show(false)
    //添加一列字符串的转array->struct->等嵌套复杂类型的值
    val df2 = df1.withColumn("jsonData", from_json($"jsonstr", arrayStruct))
    df2.printSchema()
    df2.show(false)
    val df3 = df2.select($"name",$"age",$"jsonstr",explode($"jsonData").alias("co"))
    df3.show(false)
    df3.printSchema()
    // explode可以展开array类型的字段，不能用于struct字段
    df3.select($"co.animal_interpretation").show(false)
    df3.select($"name",$"age",$"co.animal_interpretation.is_large_animal").show(false)

  }
  /*将json字段的dataframe转成map*/
  def jsonDataframeToMap(spark:SparkSession)={
      val df: DataFrame = spark.read.json(".\\src\\main\\resources\\json")
      df.printSchema()
      df.show(false)

    // 指定需要输出的类型 后面的泛型
    // 这里是隐式变量 在某处的隐式参数里面会用到 我没找到
    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    println(df.map(x => {
      x.getValuesMap[Any](List("Make", "Model")) // 返回你指定需要的字段的k-v
    }).collect().toList) // 使用了ROW.getvaluesmap
    // def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] // 官网方法定义说明：k取决于seq元素类型；v取决于你传入的泛型；这里我们传入Any
  }
  /*将map字段的dataframe转成json*/
  def mapToJson(spark:SparkSession)={
    import spark.implicits._
    import org.apache.spark.sql.functions._
    /*方法1：
    import org.apache.spark.sql.functions.to_json
    countDF.withColumn("mapfield", to_json($"mapfield"))
    方法2：
import org.apache.spark.sql.functions.udf

val convert_map_to_json = udf(
  (map: Map[String, Object]) => convertMapToJSON(map).toString
)

countDF.withColumn("mapfield", convert_map_to_json($"mapfield"))*/
  }
  /*将jsonArray的字符串变为dataframe的1个jsonArray字段，并且从中取值*/
  def strToJsonArray(spark:SparkSession)={
    import spark.implicits._
    val items = "[{\"skuId\": \"100101\", \"quantity\": 1},{\"skuId\": \"100104\", \"quantity\": 2}]"
    val mapitems=Map("items"->items)
    val mk=mapitems.keySet
    val rdd= spark.sparkContext.parallelize(Seq(mapitems))
    rdd.foreach(println)
    val df = rdd.map(x => {
      val bb = new ArrayBuffer[String]()
      for (k: String <- mk) {
        bb.+=(x(k))
      }
      bb
    }).map(x => (x(0))).toDF("items")
    df.show(false)
    //可以使用spark map将row转成对应的字段值，如x.getAs[String]("items")，然后用json库（如gson,jackson,fastjson等）进行解析
    val jsonDF = df.select(explode(from_json($"items", ArrayType(StructType(StructField("skuId", StringType) :: StructField("quantity", IntegerType):: Nil)))).as("items"))
    jsonDF.show(false)  //[100101, 1] 取出值
    jsonDF.printSchema()
    jsonDF.select($"items.skuId".as("skuId"),$"items.quantity".as("quantity")).show(false)
  }
  /*在dataframe添加一个自增列*/
  def addAutoIncrement(spark:SparkSession)={
    import spark.implicits._
    val data=Seq(("a",1,11),("b",2,22),("c",3,33)).toDF("id","num","age")
    data.withColumn("order",monotonically_increasing_id()).show(false)
  }
  /*将1列字段中的list/array[] 变成字符串*/
  def arrayToString(spark:SparkSession)={
    import spark.implicits._
    val arr=Seq(
      ("k8363","昆山","ksh","1","1435","2019-03-19 "),
      ("k1084","唐山","tsp","0","0","2019-03-19 "),
      ("k415","济南","jns","3","300","2019-03-19 "),
      ("k881","苏州","szp","0","0","2019-03-19 "),
      ("k1084","唐山","tsp","10","25","2019-03-20 "))
    val tableData=arr.toDF("train_code","station_name","station_code","is_late","late_min","arrive_date")
    val structData = tableData.groupBy( "train_code", "station_code" ).agg( collect_set( struct( "arrive_date", "late_min" ) ).as( "late_detail_set" ) )
    structData.show(false)      // [2019-03-19 :0, 2019-03-20 :25]
    structData.printSchema()

    import org.apache.spark.sql.Dataset
    val lateDetail = tableData.groupBy( "train_code", "station_code" ).agg( collect_set( concat_ws( ":", col( "arrive_date" ), col( "late_min" ) ) ).as( "late_detail_set_list" ) )
    val finalResult = lateDetail.withColumn( "date_list_str", concat_ws( ",", col( "late_detail_set_list" ) ) )
    finalResult.printSchema()
    finalResult.show( false )   //2019-03-19 :0,2019-03-20 :25
  }
  /*dataframe某一列转化为Array*/
  def oneColumnToArray(spark:SparkSession)={
    import spark.implicits._
    var data1 = Seq(
      ("0.0", "1002", "1", "1.5", "bai"),
      ("1.0", "2004", "2", "2.1", "wang"),
      ("0.0", "3007", "2", "2.1", "wang"),
      ("0.0", "4004", "3", "3.4", "wa"),
      ("1.0", "5007", "3", "3.4", "wa"),
      ("1.0", "6007", "4", "3.4", "wa"),
      ("1.0", "0",    "4", "4.7", "zhu"),
      ("0.0", "7009", "5", "1.5", "bai"),
      ("0.0", "8002", "5", "1.5", "bai"),
      ("1.0", "9004", "6", "2.1", "wang"),
      ("0.0", "10007", "6", "2.1", "wang"),
      ("0.0", "11004", "6", "3.4", "wa"),
      ("1.0", "12007", "6", "3.4", "wa"),
      ("1.0", "13007", "7", "3.4", "wa"),
      ("1.0", "14004", "7", "4.7", "zhu"),
      ("0.0", "15009", "8", "1.5", "bai"),
      ("1.0", "16009", "8", "1.5", "bai"),
      ("1.0", "17009", "8", "5.9", "wei"),
      ("0.0","18010", "12", "5.9", "wei")
    ).toDF("label", "AMOUNT", "Pclass", "name", "MAC_id")
    import org.apache.spark.sql.functions._
    data1 = data1.withColumn("AMOUNT", col("AMOUNT").cast("double"))
    data1 = data1.withColumn("name", col("name").cast("double"))
    data1 = data1.withColumn("label", col("label").cast("double"))
    data1 = data1.withColumn("Pclass", col("Pclass").cast("double"))
    data1.show(false)
    val cname="name"
    // 类型转换为Double
    def ToDouble(s:Any):Double={
      s.toString.toDouble
    }
    // collect后面接1个map(_(0)) 可以将[1.5] 这样的形式去掉[] 中括号
   var neg_tmp = data1.select(s"$cname").collect().map(_(0)).toList
    println(neg_tmp.length)
    var neg_list = data1.select(s"$cname").collect().map(_(0)).map(ToDouble).distinct.toList
    println(neg_list)
    data1.select(s"$cname").collect().foreach(println)
  }
  /*na.drop*/
  def nadrop(spark:SparkSession)={
    import spark.implicits._
    val df=Seq(("0.0","1002","1","1.5",null),("0.3","2005","2","3.4","wang"),("1.0",null,"3","5","wei"))
          .toDF("lab","amount","pclass","name","mac_id")
    /*直接用df*/
    df.drop().show(false)      //与df一样
    df.na.drop().show(false)   //去掉了有null的行
    df.drop("any").show(false)     //与df一样
    df.drop("all").show(false)      //与df一样
    df.drop("mac_id").show(false)   //与df一样
    // df.drop(1,"mac_id").show()   //报错
   // df.drop(1,Array("mac_id","amount")).show()    // 报错
   // df.drop(2).show()           //报错
   // df.drop("any",Seq("mac_id","amount")).show()  // 报错
   // df.drop("any",Array("mac_id","amount")).show()  // 报错

    println("------<<<<<<>>>>>>>------")
    println("要用dataframe.na.drop")
    val df2=df.na
    df2.drop().show()    //去掉有null或NaN的行
    df2.drop("any").show()      //去掉1行，只要改行有null或NaN的字段
    df2.drop("any",Array("mac_id","amount")).show()  // 如果mac_id和amount字段列中 任意一行有null或nan，则去掉该行
    df2.drop(Seq("amount","mac_id")).show()   //如果mac_id字段列中 有null或nan，则去掉该列
    df2.drop("all").show()     // 去掉1行，只要改行的 "所有" 字段均为null或nan
    df2.drop("all",Array("mac_id","amount")).show()   //
  }
  /*dataframe类型转换*/
  def dotUnderLineStar(spark:SparkSession)={
    import spark.implicits._
    val df=Seq(("0.0","1002","1","1.5",null),("0.3","2005","2","3.4","wang"),("1.0",null,"3","5","wei"))
      .toDF("lab","amount","pclass","name","mac_id")
    val columns=df.columns
    val arrayColumn: Array[Column]=columns.map(ele => col(ele).cast("string"))
    // :_*的作用就是，将arrayColumn数组中的每个元素作为参数传入select函数中，而不是将arrayColumn作为一个参数传入select中。
    val df2: DataFrame = df.select(arrayColumn :_*)
    df2.show(false)

    /*另一种方法*/
    val df3: DataFrame = columns.foldLeft(df){
      (currentDF, column) => currentDF.withColumn(column, col(column).cast("string"))
    }
    df3.show(false)
    df.select( $"mac_id".cast("string").as("new_mac_id")).show(false)

    /*另一个案例*/
    var data3 = Seq(
      (null, "2002", "196", "1", "bai"),
      (null, "4004", "192", "2", "wang"),
      (null, "9009", "126", "1", "bai"),
      (null, "9009", "126", "5", "wei"),
      ("1","10010", "19219", "5", "wei")
    ).toDF("label", "AMOUNT", "Pclass", "nums", "name")
    data3 = data3.withColumn("AMOUNT", col("AMOUNT").cast("int"))
    data3.show()
    //查看data3各列数据类型
    data3.dtypes.toMap.foreach(println)
    //使用take抽取3条 并转化为Seq 只有Seq才能构建DataFrame(这里toSeq和toList都可以)
    var rdd = spark.sparkContext.parallelize(data3.take(3).toSeq)
    //重新构建 DataFrame表结构 与原表对应 名称可以不同  但是对应类型必须相同  这就是前面看原表类型的原因
    val schema = new StructType().add(StructField("label", StringType, true)).add(StructField("AMOUNT",  IntegerType, true)).add(StructField("Pclass", StringType, true)).add(StructField("nums",StringType, true)).add(StructField("name", StringType, true))
    val df33 = spark.createDataFrame(rdd, schema)
    df33.show()
  }

}
