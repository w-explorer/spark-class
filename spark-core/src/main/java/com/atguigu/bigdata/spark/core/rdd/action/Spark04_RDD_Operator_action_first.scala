package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark04_RDD_Operator_action_first {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    /**
     * 返回RDD中的第一个元素
     */
    println(rdd.first())

    sc.stop()
  }

}
