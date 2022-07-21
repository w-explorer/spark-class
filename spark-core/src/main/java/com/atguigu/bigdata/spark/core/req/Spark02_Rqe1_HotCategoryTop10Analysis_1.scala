package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/5/3 9:37
 */
object Spark02_Rqe1_HotCategoryTop10Analysis_1 {

  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

    val sc = new SparkContext(sparkConf)


    //todo 热门产品排名

    //1.读取原始数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_action_data/user_visit_action.txt")

    //放置内存中，复用RDD数据。
    actionRDD.cache()
    //    actionRDD.checkpoint()
    //    actionRDD.persist()


    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )

    //2.统计产品的点击数量 (产品ID,点击数量)

    val clickCountData: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //3.统计产品的下单数量 (产品ID,下单数量)
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )

    //orderid -> [1,2,3]
    //(orderid1,1),(orderid2 ,1)
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      val cid = datas(8)
      cid.split(",").map(id => (id, 1))
    }).reduceByKey(_ + _)


    //4.统计产品的支付数量 (产品ID,支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      val cid = datas(10)
      cid.split(",").map(id => (id, 1))
    }).reduceByKey(_ + _)

    //5.综合排序取前10

    //q1: RDD重复使用
    //q2:
    // cogroup 判断数据源分区是否相同，不同就会进行shuffle，性能降低
    // if (rdd.partitioner == Some(part)) {
    //        logDebug("Adding one-to-one dependency with " + rdd)
    //        new OneToOneDependency(rdd)
    //      } else {
    //        logDebug("Adding shuffle dependency with " + rdd)
    //        new ShuffleDependency[K, Any, CoGroupCombiner](
    //          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
    //      }

    val clickNewCountRDD: RDD[(String, (Int, Int, Int))] = clickCountData.map {
      case (cid, count) => {
        (cid, (count, 0, 0))
      }
    }
    val orderNewCountRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, count) => {
        (cid, (0, count, 0))
      }
    }
    val payNewCountRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, count) => {
        (cid, (0, 0, count))
      }
    }

    val sourceRDD: RDD[(String, (Int, Int, Int))] = clickNewCountRDD.union(orderNewCountRDD).union(payNewCountRDD)
    val analysisRDD = sourceRDD.reduceByKey {
      (t1,t2) => {
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    }

    val resultData: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


    //6.将结果数据打印出来
    resultData.foreach(println)


    sc.stop()

    val endTime: Long = System.currentTimeMillis()

    println(endTime-startTime)
  }

}
