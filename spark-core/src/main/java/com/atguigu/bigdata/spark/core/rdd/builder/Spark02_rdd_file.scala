package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *        集合中（内存）创建RDD
 *
 */
object Spark02_rdd_file {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sparkContext = new SparkContext(sparkConf)


    //TODO 创建 RDD
    val seq = Seq[Int](1, 2, 3)


    //从文件中创建RDD 将文件中的的数据作为处理的数据源
    //path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
//    val rdd: RDD[String] = sparkContext.textFile("D:\\workspaces\\idea\\spark-class\\datas\\sparkData\\1.txt")

//    val rdd: RDD[String] = sparkContext.textFile("datas/sparkData/1.txt")

    //path 可以是文件的具体路径，也可以是目录名称  读取目录下的所有文件
//    val rdd: RDD[String] = sparkContext.textFile("datas")


    //path 也可以是通配符  *
    val rdd: RDD[String] = sparkContext.textFile("/datas/sparkData/*.txt")

    //path 也可以是分布式存储系统的路径  ： HDFS

//    val rdd: RDD[String] = sparkContext.textFile("hdfs://linux1:8082/text.txt")

    rdd.collect().foreach(println)


    //TODO 关闭环境

    sparkContext.stop()
  }
}
