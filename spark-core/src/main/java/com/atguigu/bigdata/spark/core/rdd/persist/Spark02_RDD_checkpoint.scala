package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/19 20:46
 */
object Spark02_RDD_checkpoint {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")


    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))


    val flatRdd: RDD[String] = rdd.flatMap(list => {
      list.split(" ")
    })

    val mapRdd: RDD[(String, Int)] = flatRdd.map(word => {
      println("@@@@@@@@@@@")
      (word, 1)
    })


    /**
     * 需要落盘
     * 在执行之后，数据不会被删除
     * 一般保存数据文件，都存储在分布式存储文件系统中  HDFS
     *
     */
    mapRdd.checkpoint()

    mapRdd.reduceByKey(_+_).collect().foreach(println)

    println("****************************")

    mapRdd.groupByKey().collect().foreach(println)





  }
}
