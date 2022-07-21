package com.atguigu.bigdata.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *   文件保存
 *
 */
object Spark03_rdd_acc {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      1,2,3,4
    ))

    /**
     * 获取系统累加器
     * 默认提供了简单数据聚合的累加器
     */

    val sum: LongAccumulator = sc.longAccumulator("sum")

//    sc.doubleAccumulator  浮点型
//    sc.collectionAccumulator  集合型


    val mapRdd: RDD[Int] = rdd.map(num => {
      //使用累加器
      sum.add(num)
      num
    })

    /**
     * 少加：转换算子调用累加器，如果没有行动算子的话，那么不会执行
     * 多加：转换算子调用累加器，如果有多个行动算子的话，那么多次执行
     *      可以配合 catch来解决
     */


    mapRdd.collect()
    mapRdd.collect()




    println(sum.value)

    //关闭环境
    sc.stop()
  }
}
