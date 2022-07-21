package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/5/3 9:37
 */
object Spark03_Rqe1_HotCategoryTop10Analysis_2 {

  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

    val sc = new SparkContext(sparkConf)


    //todo 热门产品排名
    //q1:存在大量的shuffle 操作，reduceByKey 内部中有shuffle操作，spark为了优化，底层做了缓存。多链路中读取数据.

    //1.读取原始数据
    val actionRDD = sc.textFile("datas/user_action_data/user_visit_action.txt")


    val sourceRDD = actionRDD.flatMap(
      action => {
        val datas  = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val cids: Array[String] = datas(8).split(",")
          cids.map(
            cid => (cid, (0, 1, 0))
          )
        } else if (datas(10) != "null") {
          val cids: Array[String] = datas(10).split(",")
          cids.map(
            cid => (cid, (0, 0, 1))
          )
        } else {
          Nil
        }
      }
    )

    val analysisRDD = sourceRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    val resultData: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


    //6.将结果数据打印出来
    resultData.foreach(println)


    sc.stop()

    val endTime: Long = System.currentTimeMillis()

    println(endTime - startTime)
  }

}
