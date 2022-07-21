package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark16_RDD_Operator_Transform_foldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)),2)

    /**
     * foldByKey:
     * 分区内和分区间的计算规则一致
     *
     * // TODO : 取出每个分区内相同 key 的最大值然后分区间相加
     */

    val dataRdd: RDD[(String, Int)] = rdd.foldByKey(2)(
      (x, y) => math.max(x, y),
    )
    dataRdd.foreach(println)

    sc.stop()
  }

}
