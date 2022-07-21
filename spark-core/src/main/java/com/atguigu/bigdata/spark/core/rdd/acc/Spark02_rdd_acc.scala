package com.atguigu.bigdata.spark.core.rdd.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *   文件保存
 *
 */
object Spark02_rdd_acc {

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


    rdd.foreach(num =>{
      sum.add(num)
    })

    println(sum.value)

    //关闭环境
    sc.stop()
  }
}
