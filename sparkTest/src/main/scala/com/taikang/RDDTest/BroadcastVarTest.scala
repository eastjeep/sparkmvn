package com.taikang.RDDTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
//假如在执行map操作时，在某个Worker的一个Executor上有分配5个task来进行计算，
// 在不使用广播变量的情况下，因为Driver会将我们的代码通过DAGScheduler划分会不同stage，交由taskScheduler，
// taskScheduler再将封装好的一个个task分发到Worker的Excutor中，也就是说这个过程当中，我们的genderMap也会被封装到这个task中，
// 显然这个过程的粒度是task级别的，每个task都会封装一个genderMap，在该变量数据量不大的情况下，是没有问题的，
// 当数据量很大时，同时向一个Excutor上传递5份这样相同的数据，这是很浪费网络中的带宽资源的；
// 广播变量的使用可以避免这一问题的发生，将genderMap广播出去之后，其只需要发送给Excutor即可，
// 它会保存在Excutor的BlockManager中，此时，Excutor下面的task就可以共享这个变量了，这显然可以带来一定性能的提升。
//注意：：
//1. 不能广播RDD，但是可以将RDD的结果广播出去
//2. 广播变量只能在Driver端定义，在Excutor端使用，不能在Executor端更改变量值
//3. 如果不使用广播变量，在Executor端有几个task，就会有几个Driver端变量的副本；使用广播变量，在每个Executor中只有一个Driver端的副本，由BlockManager对象管理

object BroadcastVarTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("broadcastTest")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val userList = List(
      "001,刘向前,18,0",
      "002,冯  剑,28,1",
      "003,李志杰,38,0",
      "004,郭  鹏,48,2"
    )

    val genderMap = Map("0" -> "女", "1" -> "男")

    val genderMapBC:Broadcast[Map[String, String]] = sc.broadcast(genderMap)

    val userRDD = sc.parallelize(userList)
    val retRDD = userRDD.map(info => {
      val prefix = info.substring(0, info.lastIndexOf(","))   // "001,刘向前,18"
      val gender = info.substring(info.lastIndexOf(",") + 1)
      val genderMapValue:Map[String, String] = genderMapBC.value  // 这里广播变量返回的是原来的Map
      val newGender = genderMapValue.getOrElse(gender, "男")
      prefix + "," + newGender
    })
    retRDD.foreach(println)
    sc.stop()
  }
}
