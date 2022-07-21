package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/19 20:46
 */
object Spark03_RDD_区别 {


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
     * catch :将数据存储在内存中进行数据重用
     *     数据不安全，重启或者被清除了等
     *
     * persist: 将数据临时存储在磁盘文件中进行数据重用
     *     落盘了但是有磁盘io性能低，但是安全
     *     如果作业执行完毕，临时保存的文件就会删除，丢失
     *
     * checkpoint； 将数据长久的保存在磁盘文件中进行数据重用
     *    涉及磁盘IO，性能底，数据安全  还可以跨作业执行
     *    为了保证数据安全，一般情况下，会独立执行作业;导致执行两次RDD的计算，性能降低
     *    为了提升性能，一般情况下，是需要和cache配合使用的。 内存中保存一份，RDD就只执行一次
     *
     */
    mapRdd.cache()
    mapRdd.checkpoint()

    mapRdd.reduceByKey(_+_).collect().foreach(println)

    println("****************************")

    mapRdd.groupByKey().collect().foreach(println)





  }
}
