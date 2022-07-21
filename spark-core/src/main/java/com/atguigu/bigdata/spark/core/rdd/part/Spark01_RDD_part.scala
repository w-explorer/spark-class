package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_part {

  /**
   * 自定义分区器
   *
   */

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "11"),
      ("cba", "22"),
      ("wba", "33"),
      ("yyynba", "44")
    ))

//    val rdd: RDD[String] = sc.makeRDD(List("nba", "cba", "wba"))

    //必须k v 类型RDD 才能使用分区器
    val myPartitionRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    myPartitionRdd.saveAsTextFile("output")


    sc.stop()
  }

}

/**
 * 1.继承  Partitioner
 * 2.重写方法
 */
class MyPartitioner extends Partitioner{
  //可以写死 也可以外面传进来
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    key match {
      case "nba" => 0
      case "cba" => 1
      case _ =>2
    }
  }
}
