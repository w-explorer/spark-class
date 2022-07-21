package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_action_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    /**
     * 将RDD中所有数据聚合，先聚合分区内的数据再聚合分区间的数据
     */

    val dataRdd: Int = rdd.reduce((x, y) => x + y)

    println(dataRdd)
    sc.stop()
  }

}
