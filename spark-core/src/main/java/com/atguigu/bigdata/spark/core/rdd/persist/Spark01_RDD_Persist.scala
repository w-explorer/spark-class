package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/19 20:46
 */
object Spark01_RDD_Persist {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))


    val flatRdd: RDD[String] = rdd.flatMap(list => {
      list.split(" ")
    })

    val mapRdd: RDD[(String, Int)] = flatRdd.map(word => {
      println("@@@@@@@@@@@")
      (word, 1)
    })


    /**
     * RDD 不存数据
     * RDD可以重用，但是数据不能重用，重用RDD 会重新再运行一次数据
     *
     * 需要用到缓存
     *
     * 在数据执行较长，或数据执行比较重要的场景  可以重用
     */

    mapRdd.cache()
//    mapRdd.persist(StorageLevel.DISK_ONLY)


    mapRdd.reduceByKey(_+_).collect().foreach(println)

    println("****************************")

    mapRdd.groupByKey().collect().foreach(println)





  }
}
