package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark13_RDD_Operator_Transform_partitionBy {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(1,2,3,4))


    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))

    /**
     * partitionBy:
     * 将数据按照指定的Partitioner 重新分区。Spark 默认分区器是HashPartitioner
     */

    val dataRdd: RDD[(Int, Int)] = mapRdd.partitionBy(new HashPartitioner(2))


    dataRdd.saveAsTextFile("outPut")


    sc.stop()
  }

}
