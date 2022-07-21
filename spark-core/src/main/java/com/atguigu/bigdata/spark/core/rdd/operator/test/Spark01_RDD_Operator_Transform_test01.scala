package com.atguigu.bigdata.spark.core.rdd.operator.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_Transform_test01 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    /**
     * 1) 数据准备
     * agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 2) 需求描述
     * 统计出每一个省份每个广告被点击数量排行的 Top3
     * 3) 需求分析
     * 4) 功能实现
     */


    //1.数据准备  时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    val rdd: RDD[String] = sc.textFile("datas/transform/agent.log")

    //2.数据组合  （（省份，广告），1）

    val mapRdd: RDD[((String, String), Int)] = rdd.map {
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    }

    //3.每个省份的广告wordcount  （(省份，广告)，sum）
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    //4.每个省份的广告分组 （省份，(广告，sum）)
    val newRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((prd, ad), sum) => {
        (prd, (ad, sum))
      }
    }

    //按照身份分组 （省份，（广告1，sum）,(广告2，sum)）

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newRdd.groupByKey()
    //5.省份之间的广告排序  （省份，（（广告1，sum），（广告2，sum）））

    val dataRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        val list: List[(String, Int)] = iter.toList
        list.sortBy(_._2).reverse.take(3)
      }
    )

    //打印数据
    dataRdd.foreach(println)

    sc.stop()
  }

}
