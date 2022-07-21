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
object Spark02_rdd_file1 {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sparkContext = new SparkContext(sparkConf)


    //TODO 创建 RDD
    val seq = Seq[Int](1, 2, 3)


    //从文件中创建RDD 将文件中的的数据作为处理的数据源

    //textFile 是以行为单位
    //wholeTextFiles 是以文件为单位
    // 读取结果表示为元组。第一个元数表示文件的路径，第二个表示文件内容
    val rdd: RDD[(String, String)] = sparkContext.wholeTextFiles("datas/sparkData")

    rdd.collect().foreach(println)


    //TODO 关闭环境

    sparkContext.stop()
  }
}
