package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark04_RDD_Operator_Transform_flatMap_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello scala"))


    /**
     * flatMap:讲处理的数据进行扁平化处理后再进行处理，也称之为扁平化映射
     *
     */
    val mapRdd: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
