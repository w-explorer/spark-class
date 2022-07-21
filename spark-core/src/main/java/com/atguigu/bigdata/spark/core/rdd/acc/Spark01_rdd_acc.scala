package com.atguigu.bigdata.spark.core.rdd.acc

import java.util
import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *   文件保存
 *
 */
object Spark01_rdd_acc {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      1,2,3,4
    ))

    /**
     * 分区间计算，分区里计算
     */
    val reduceRdd: Int = rdd.reduce(_ + _)
    println(reduceRdd)


    var sum  = 0

    rdd.foreach(num =>{
      println(num)
      sum += num
    })

    println(sum)

    //关闭环境
    sc.stop()
  }
}
