package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark11_RDD_Operator_Transform_subtract {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd1 = sc.makeRDD(List(1,2,3))
    val rdd2 = sc.makeRDD(List(3,5,6))


    /**
     * subtract:
     * 对源RDD和参数RDD求差集后返回一个新的RDD
     */


    val dataRdd: RDD[Int] = rdd1.subtract(rdd2)

    println(dataRdd.collect().mkString(","))


    sc.stop()
  }

}
