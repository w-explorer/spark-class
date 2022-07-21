package com.atguigu.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author wencheng
 * @create 2022/5/15 20:50
 */
object Spark04_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {
    //tudo环境准备
    val sc = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sc).getOrCreate()

    //tudo执行逻辑

    //方式 1：通用的 load 方法读取
//    val df: DataFrame = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://10.0.8.104:53302/104test")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "sdc")
//      .option("password", "Cdsf_sjzl_5408")
//      .option("dbtable", "student")
//      .load()
//
//    df.show
//
//    //方式 2:通用的 load 方法读取 参数另一种形式
//    spark.read.format("jdbc")
//      .options(Map("url"->"jdbc:mysql://10.0.8.104:53302/104test?user=sdc&password=Cdsf_sjzl_5408",
//        "dbtable"->"student",
//        "driver"->"com.mysql.cj.jdbc.Driver")).load().show


    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "sdc")
    props.setProperty("password", "Cdsf_sjzl_5408")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://10.0.8.104:53302/104test", "student", props)
    df.show

    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.0.8.104:53302/104test", "student1", props)









    //tudo关闭环境
    spark.close()

  }
}
