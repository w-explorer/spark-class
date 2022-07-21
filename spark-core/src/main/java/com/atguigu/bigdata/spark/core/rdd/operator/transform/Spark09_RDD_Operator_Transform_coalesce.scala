package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark09_RDD_Operator_Transform_coalesce {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)


    /**
     * coalesce: 根据梳理缩减分区，用于大数据集过滤后，提高小数据集的执行效率。收缩合并分区，减少任务调度成本
     *
     * 默认情况下数据不会被打乱重新组合,可能出现数据倾斜
     * 想要打乱重组，避免数据倾斜，设置第二个
     *
     * 想要扩大分区怎么办？
     */
//    val newRdd: RDD[Int] = rdd.coalesce(2)
    val newRdd: RDD[Int] = rdd.coalesce(2,true)

    newRdd.saveAsTextFile("output")


    sc.stop()
  }

}
