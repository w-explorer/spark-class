package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark09_RDD_Operator_Transform_coalesce_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)


    /**
     * coalesce: 可以扩大分区，但是不shuffle是没有意义的，不起作用
     * 想要实现扩大分区效果，需要使用shuffle操作
     *
     * spark提供了一个简化的操作
     * 缩减分区：coalesce
     * 扩大分区：repartition 底层也是用的coalesce
     */
//    val newRdd: RDD[Int] = rdd.coalesce(2)

    val newRdd: RDD[Int] = rdd.repartition(3)

    newRdd.saveAsTextFile("output")


    sc.stop()
  }

}
