package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark07_RDD_Operator_action_aggregate {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    /**
     * 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
     * 参数一：初始值
     * 参数二：分区内的操作
     * 参数三: 分区间的操作
     *
     */

    val dataRdd: Int = rdd.aggregate(2)(_ + _, _ + _)


    println(dataRdd)

    sc.stop()
  }

}
