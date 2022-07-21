package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark05_RDD_Operator_Transform_groupby_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List("hello","spark","nihao","scala"),2)


    /**
     * groupBy: 将数据根据指定的规则进行分组，分区默认不变，但是数据会被重新组合，
     *
     * 一个组的数据在一个分区中，但是并不是说一个分区只有一个组。
     *
     */

    //按照搜字母分组

    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(
      s => {
        s.charAt(0)
      }
    )

    groupRdd.collect().foreach(println)




    sc.stop()
  }

}
