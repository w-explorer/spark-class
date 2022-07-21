package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark05_RDD_Operator_Transform_glom {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(1,2,3,4),2)


    /**
     * glom:将同一个分区的数据转换为相同类型的内存数据组进行处理。分区不变
     *
     *
     * flatMap将整体变成个体
     * glom将个体变成整体
     *
     */

    val glomRdd: RDD[Array[Int]] = rdd.glom()

    glomRdd.collect().foreach(data=>println(data.mkString(",")))

    sc.stop()
  }

}
