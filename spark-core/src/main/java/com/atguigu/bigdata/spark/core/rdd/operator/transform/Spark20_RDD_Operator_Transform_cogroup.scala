package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark20_RDD_Operator_Transform_cogroup {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子

    /**
     * cogroup: 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
     */

    val dataRDD1 = sc.makeRDD(List(("a",1),("a",2),("c",3),("d",3)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("c",2),("c",3)))
    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] =
      dataRDD1.cogroup(dataRDD2)

    value.collect().foreach(println)

    sc.stop()
  }

}
