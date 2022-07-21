package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark09_RDD_Operator_action_Lineage_血缘 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子

    /**
     *LineAge 记录元数据转换的数据和转换行为，当该RDD部分分区数据丢失时，它可以根据这些信息重新运算和恢复丢失的数据分区。
     *
     * 每个RDD会保存血缘关系，为了提升容错性
     * RDD不会保存数据。
     *
     */

    val fileRDD: RDD[String] = sc.textFile("datas/input/word.txt")
    println(fileRDD.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)

    println("----------------------")
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)
    resultRDD.collect()

    sc.stop()
  }
}
