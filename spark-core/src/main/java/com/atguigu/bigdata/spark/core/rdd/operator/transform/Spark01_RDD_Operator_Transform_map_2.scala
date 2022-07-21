package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_Transform_map_2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[String] = sc.makeRDD(List("2", "3"))


    /**
     * map 把数据逐条进行映射处理，映射可以转换类型，也可以是值的映射
     */
    val mapRdd: RDD[Int] = rdd.map(
      s => {
        Integer.parseInt(s)
      }
    )

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
