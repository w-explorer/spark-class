package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark08_RDD_Operator_Transform_distinct {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,2,3),1)


    /**
     * distinct: 去重
     */

//    val dataRdd: RDD[Int] = rdd.distinct()
    val dataRdd: RDD[Int] = rdd.distinct(2)

    println(dataRdd.collect().mkString(","))

    sc.stop()
  }

}
