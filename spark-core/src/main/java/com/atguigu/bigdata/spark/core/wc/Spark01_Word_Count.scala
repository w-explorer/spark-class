package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/24 10:04
 */
object Spark01_Word_Count {

  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和Spark框架的连接
    //类似mysql驱动的连接  JDBC:Connection

    //指定spark运行位置                          local模式
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)


    //TODO 执行业务操作

    //TODO 关闭连接
    sc.stop()

  }

}
