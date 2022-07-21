package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author wencheng
 * @create 2022/5/15 20:50
 */
object Spark02_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {
    //tudo环境准备
    val sc = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._

    //tudo执行逻辑

    //DataFrame
    val df: DataFrame = spark.read.json("datas/input/user.json")

    df.createOrReplaceTempView("user")

    spark.sql("select username from user").show

    spark.udf.register("prefixName",(name:String) => {
      "Name:" + name
    })

    //给字段加上自定义前缀
    spark.sql("select prefixName(username) from user").show









    //tudo关闭环境
    spark.close()

  }
}
