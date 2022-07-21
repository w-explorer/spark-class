package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark17_RDD_Operator_Transform_sortByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)))

    /**
     * foldByKey:
     * 分区内和分区间的计算规则一致
     *
     */

    rdd.sortByKey(true).collect().foreach(println)


    sc.stop()
  }

}
