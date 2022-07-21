package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark09_RDD_Operator_action_依赖 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    /**
     * 相邻两个RDD关系称之为 依赖关系；新的RDD依赖旧的RDD
     *
     * A->B->C   多个连续RDD依赖关系称之为血缘关系
     */

    val fileRDD: RDD[String] = sc.textFile("datas/input/word.txt")
    println(fileRDD.dependencies)
    println("----------------------")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)
    resultRDD.collect()

    sc.stop()
  }
}
