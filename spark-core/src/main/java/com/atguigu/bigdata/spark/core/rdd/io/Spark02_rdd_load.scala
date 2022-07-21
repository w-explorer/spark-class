package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *   文件保存
 *
 */
object Spark02_rdd_load {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd1: RDD[String] = sc.textFile("output1")
    println(rdd1.collect().mkString(","))


    val rdd2: RDD[(String, String)] = sc.objectFile[(String, String)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3: RDD[(String, String)] = sc.sequenceFile[String, String]("output3")
    println(rdd3.collect().mkString(","))

    //关闭环境
    sc.stop()
  }
}
