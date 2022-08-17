package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author wencheng
 * @create 2022/5/15 20:50
 */
object Spark05_SparkSQL_HIVE {

  def main(args: Array[String]): Unit = {
    //tudo环境准备
    val sc = new SparkConf().
      setMaster("local[*]").
      setAppName("SparkSQL")
    //创建 SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sc)
      .getOrCreate()

    spark.sql("show databases;").show()








    //tudo关闭环境
    spark.close()

  }
}
