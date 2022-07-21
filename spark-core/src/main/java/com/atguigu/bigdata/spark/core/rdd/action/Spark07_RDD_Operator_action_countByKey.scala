package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark07_RDD_Operator_action_countByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",1),("c",2)),2)

    /**
     *统计每个key的个数
     */

    val dataRdd: collection.Map[String, Long] = rdd.countByKey()

    println(dataRdd)

    sc.stop()
  }

}
