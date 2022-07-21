package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark16_RDD_Operator_Transform_aggregateByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)),2)

    /**
     * aggregateByKey:
     * 将数据数据进行分组，不能做聚合操作。没有数据量的减少
     *
     * // TODO : 取出每个分区内相同 key 的最大值然后分区间相加
     * // aggregateByKey 算子是函数柯里化，存在两个参数列表
     * // 1. 第一个参数列表中的参数表示初始值
     * // 2. 第二个参数列表中含有两个参数
     * // 2.1 第一个参数表示分区内的计算规则
     * // 2.2 第二个参数表示分区间的计算规则
     */

    val dataRdd: RDD[(String, Int)] = rdd.aggregateByKey(2)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    dataRdd.foreach(println)

    sc.stop()
  }

}
