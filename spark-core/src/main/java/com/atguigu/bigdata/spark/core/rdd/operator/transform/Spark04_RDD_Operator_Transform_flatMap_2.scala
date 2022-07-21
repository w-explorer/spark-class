package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark04_RDD_Operator_Transform_flatMap_2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))


    /**
     * flatMap:讲处理的数据进行扁平化处理后再进行处理，也称之为扁平化映射
     *
     * match case 模式匹配；java中的泛型 上诉下溯  instanceof
     *
     */
    val flatMapRdd: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[Any] => list
          case dat => List(dat)
        }
      }
    )


    flatMapRdd.collect().foreach(println)

    sc.stop()
  }

}
