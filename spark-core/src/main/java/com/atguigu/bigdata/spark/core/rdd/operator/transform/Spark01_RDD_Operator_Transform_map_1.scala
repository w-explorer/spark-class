package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_Transform_map_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val dates: RDD[String] = sc.textFile("datas/transform/apache.log")

    val mapRdd: RDD[String] = dates.map(line => {
      val dates: Array[String] = line.split(" ")
      dates(6)
    })

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
