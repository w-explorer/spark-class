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
object Spark01_rdd_save {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "11"),
      ("cba", "22"),
      ("wba", "33")
    ))
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    //关闭环境
    sc.stop()
  }
}
