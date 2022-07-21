package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark10_RDD_Operator_Transform_sortBy {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(2,3,1,4,5,6),2)


    /**
     * sortBy:
     * 可以通过函数进行数据处理，之后按照函数处理后的结果进行排序，默认为升序，排序后新产生的RDD的分区数与原RDD的分区一致。
     * 中间存在shuffle过程
     */
    val dataRdd: RDD[Int] = rdd.sortBy(
      num => num,false,1
    )


    dataRdd.collect().foreach(println)

    dataRdd.saveAsTextFile("outPut")


    sc.stop()
  }

}
