package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
 * @author wencheng
 * @create 2022/3/24 10:04
 */
object Spark04_Word_Count {

  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和Spark框架的连接
    //类似mysql驱动的连接  JDBC:Connection

    //指定spark运行位置                          local模式
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作

    //1.读取文件数据  按行读取
    val line = sc.textFile("datas/input")

    //2.切分单词  扁平转换
    val words: RDD[String] = line.flatMap(_.split(" "))


    val wordToOne: RDD[(String, Int)] = words.map(word => {
      (word, 1)
    })


    //spark 提供了很多可以将分组和聚合使用一个方法实现
    //reduceByKey
    // wordToOne.reduceByKey((x,y)=>{x+y}) 匿名函数简化，顺序执行且参数只出现一次可以用_代替
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val wordCount: Array[(String, Int)] = wordToCount.collect()

    wordCount.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }

}
