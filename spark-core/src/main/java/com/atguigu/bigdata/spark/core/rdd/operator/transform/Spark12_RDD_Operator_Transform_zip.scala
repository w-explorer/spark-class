package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark12_RDD_Operator_Transform_zip {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd1 = sc.makeRDD(List(1,2,3),1)
    val rdd2 = sc.makeRDD(List(3,5,6),1)


    /**
     * zip:
     * 将两个RDD中的元素，以键值对的形式进行合并。其中键值对中的key为第一个RDD中的元素，Value为第二个RDD中的相同位置的元素
     *
     * 两个RDD分区数量以及数据量必须一样。
     *
     * 数据类型可以不一致
     */


    val dataRdd: RDD[(Int, Int)] = rdd1.zip(rdd2)

    dataRdd.collect().foreach(println)


    sc.stop()
  }

}
