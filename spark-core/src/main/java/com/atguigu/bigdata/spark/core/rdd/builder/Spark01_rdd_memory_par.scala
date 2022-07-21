package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *        RDD 分区
 *
 */
object Spark01_rdd_memory_par {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","3")

    val sparkContext = new SparkContext(sparkConf)


    //TODO 创建 RDD
    val seq = Seq[Int](1, 2, 3)


    //numSlices number of partitions to divide the collection into
    //makeRDD 第一个参数是数据，第二个是分区数量 可以不填写为默认值  defaultParallelism
    //     scheduler.conf.getInt("spark.default.parallelism", totalCores)
    val rdd: RDD[Int] = sparkContext.makeRDD(seq)

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    //TODO 关闭环境

    sparkContext.stop()
  }
}
