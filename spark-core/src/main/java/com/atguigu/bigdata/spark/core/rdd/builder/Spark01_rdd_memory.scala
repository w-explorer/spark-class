package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *        集合中（内存）创建RDD
 *
 */
object Spark01_rdd_memory {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sparkContext = new SparkContext(sparkConf)


    //创建 RDD
    val seq = Seq[Int](1, 2, 3)

    //parallelize  :  并行 | 并行数 例如两个任务 但是有8个核心数，并行数 为2；    4个任务 两个核心，并行数为2

    //从集合中创建RDD 将内存中的集合的数据作为处理的数据源
//    val rdd: RDD[Int] = sparkContext.parallelize(seq)


    val rdd: RDD[Int] = sparkContext.makeRDD(seq)

    rdd.collect().foreach(println)


    //关闭环境

    sparkContext.stop()
  }
}
