package com.atguigu.bigdata.spark.core.rdd.save

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_save_saveAsTestFile {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    /**
     * 保存成 text文件
     */
//    rdd.saveAsTextFile("output")
    /**
     * 序列化成对象保存到文件
     */
//    rdd.saveAsObjectFile("output")

    /**
     * 保存成SequenceFile
     * 要求数据的格式必须是  k-v 类型
     */
    rdd.map{(_,1)}.saveAsSequenceFile("output")
    sc.stop()
  }

}
