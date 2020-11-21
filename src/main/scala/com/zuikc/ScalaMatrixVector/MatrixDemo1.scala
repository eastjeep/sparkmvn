package com.zuikc.ScalaMatrixVector

import breeze.linalg.{DenseMatrix => BDM}

object MatrixDemo1 {
  def main(args: Array[String]): Unit = {
    val a :BDM[Double] = BDM.rand(2,10)
    val b :BDM[Double] = BDM.rand(2,10)
    val a1 = a(0,::)
    val b1 = b(0,::)
    val a5 = a.data(4)
    val b7 = b.data(6)
    //注意1、索引是从0开始的，所以第5个元素的索引值为4
    //注意2、矩阵的存储是按列存储的，也就是第一列之后接着是第二列的数据，并不是按照行来存储的，这一点和MATLAB一样
    println(a5)
    println("-------->>>>>>>")
    println(b7)
  }
}
