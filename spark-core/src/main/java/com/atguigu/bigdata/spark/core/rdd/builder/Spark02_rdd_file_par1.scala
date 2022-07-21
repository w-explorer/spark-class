package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *        RDD 文件源  分区数据分配
 *
 */
object Spark02_rdd_file_par1 {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","3")

    val sparkContext = new SparkContext(sparkConf)


    //TODO 创建 RDD


    //1.数据以行为单位读取
    //  spark读取文件，采用得是hadoop的方式读取，所以一行行读取。和字节数没有关系
    //2.数据读取是以偏移量为单位,偏移量不会被重复读取
    /**
     * 1CRLF =>012
     * 2CRLF =>345
     * 3   =>6
     *
     */
    //3.数据分区的偏移量范围的计算  steep是分区数
    /**
     * 0=> [0,3] =>12
     * 1=> [3,6] =>3
     * 2=> [6,7] =>
     */

    //如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd: RDD[String] = sparkContext.textFile("datas/rddFile/1.txt",2)

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //TODO 关闭环境

    sparkContext.stop()
  }
}
