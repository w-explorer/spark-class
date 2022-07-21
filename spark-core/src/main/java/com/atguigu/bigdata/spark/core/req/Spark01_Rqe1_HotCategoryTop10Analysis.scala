package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/5/3 9:37
 */
object Spark01_Rqe1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

    val sc = new SparkContext(sparkConf)

    //1.读取原始数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_action_data/user_visit_action.txt")


    //todo 热门产品排名

    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountData: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //2.统计产品的点击数量 (产品ID,点击数量)

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
    //COGROUP = connect+group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountData.cogroup(orderCountRDD, payCountRDD)

    //5.综合排序取前10
    //点击数量->下单->支付
    //元组排序： 先比较第一个，再比较第二个，以此类推
    //（产品id,(点击数量，下单数量，支付数量)）
    //join zip leftOuterJoin cogroup
    //join 需要每个k-v 中要存在k的元素，但是 这里有点击不一定有下单和支付数据
    //zip 拉链 和数据k的位置有关
    //leftOuterJoin 以左边表为主表，可能左边数据没有  不确定 用right或left
    //cogroup 在每个数据中分组，即使没有数据也会 分组


    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCount = 0;
        var orderCount = 0;
        var payCount = 0;

        val iter1: Iterator[Int] = clickIter.iterator
        if (iter1.hasNext) {
          clickCount = iter1.next()
        }
        val iter2: Iterator[Int] = orderIter.iterator
        if (iter2.hasNext) {
          orderCount = iter2.next()
        }
        val iter3: Iterator[Int] = payIter.iterator
        if (iter3.hasNext) {
          payCount = iter3.next()
        }

        (clickCount, orderCount, payCount)
      }
    }

    val resultData: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


//    clickCountData.collect().foreach(println)
//    println("1111111111111111")
//    orderCountRDD.collect().foreach(println)
//    println("22222222222222222")
//    payCountRDD.collect().foreach(println)



    //6.将结果数据打印出来
    resultData.foreach(println)





    sc.stop()
    val endTime: Long = System.currentTimeMillis()

    println(endTime-startTime)

  }
}
