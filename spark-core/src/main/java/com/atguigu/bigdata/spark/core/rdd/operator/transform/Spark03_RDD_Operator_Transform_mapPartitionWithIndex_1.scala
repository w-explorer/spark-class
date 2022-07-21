package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark03_RDD_Operator_Transform_mapPartitionWithIndex_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    /**
     * mapPartitionsWithIndex
     * 以分区为单位，将数据发往计算节点进行处理，在处理的同时可以获取当前分区索引
     *
     */


    //获取每个数据的分区索引值
    /**
     * (1,0)
     * (2,1)
     * (3,2)
     * ....
     */

    val mapRdd = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => {
            (num, index)
          }
        )
      }
    )

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
