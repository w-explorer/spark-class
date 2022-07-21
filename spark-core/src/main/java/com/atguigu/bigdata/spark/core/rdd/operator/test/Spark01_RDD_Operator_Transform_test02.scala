package com.atguigu.bigdata.spark.core.rdd.operator.test

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_Transform_test02 {

  def main(args: Array[String]): Unit = {
    val ids = List(1,2,3,4,5)
    val list: List[(Int, Int)] = ids.zip(ids.tail)
    list.foreach(println)
    ids.tail.foreach(println  )
  }

}
