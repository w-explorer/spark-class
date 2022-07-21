package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark18_RDD_Operator_Transform_join {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子

    /**
     * join: 在类型为（k,v）(k,w)的RDD上调用，返回一个key对应的所有元素连接在一起的（k,(v,w)）的RDD
     *
     * 只连接key相同的元素
     */

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (3, 5), (5, 6)))
    rdd.join(rdd1).collect().foreach(println)


    sc.stop()
  }

}
