package com.atguigu.bigdata.spark.core.rdd.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *   广播变量
 *   1.k-v 数据进行 join时，导致计算数据几何增长，影响shuffle性能
 *
 *   如果有场景会对被一个分区传输一个大数据量的对象作为应用。可以用广播变量，分区之间共享使用。
 *
 */
object Spark05_rdd_bc {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

    val map = mutable.Map(("a",2),("b",3),("c",4))
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map{
      case (k, v) => {
        val count: Int = bc.value.getOrElse(k, 0)
        (k,(v,count))
      }
    }.collect().foreach(println)






    //关闭环境
    sc.stop()
  }
}
