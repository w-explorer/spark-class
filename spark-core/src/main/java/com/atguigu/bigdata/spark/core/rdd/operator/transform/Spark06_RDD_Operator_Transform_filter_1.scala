package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark06_RDD_Operator_Transform_filter_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子

    val fileRdd: RDD[String] = sc.textFile("datas/transform/apache.log")

    /**
     * filter: 将数据按照指定的规则进行数据过滤，满住的保留，不满足的丢弃。
     * 数据过滤后，分区不变，但是分区内的数据会出现数据不均衡，生成环境下，可能造成   数据倾斜
     *
     * 获取 固定时间段类的 2015 5 17 的日志
     *
     */

    val filterRdd: RDD[String] = fileRdd.filter(
      line => {
        val words: Array[String] = line.split(" ")
        val str: String = words(3)
        str.startsWith("17/05/2015")
      }
    )


    filterRdd.collect().foreach(println)



    sc.stop()
  }

}
