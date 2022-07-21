package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark15_RDD_Operator_Transform_groupByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(("a",1),("b",1),("b",2)))

    /**
     * groupByKey:
     * 将数据数据进行分组，不能做数据的聚合操作。没有数据量的减少
     */

   rdd.groupByKey().collect().foreach(println)

    sc.stop()
  }

}
