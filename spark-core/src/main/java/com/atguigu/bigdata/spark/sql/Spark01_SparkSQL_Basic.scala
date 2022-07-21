package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author wencheng
 * @create 2022/5/15 20:50
 */
object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {
    //tudo环境准备
    val sc = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._

    //tudo执行逻辑

    //DataFrame
//    val df: DataFrame = spark.read.json("datas/input/user.json")
//    println("dataFrame")
//    df.show()

    //DataFrame to sql

//    df.createOrReplaceTempView("user")
//
//    spark.sql("select * from user").show()
//    spark.sql("select username,age from user").show()
//    spark.sql("select avg(age) from user").show()

    //DataFrame to Dsl
    //在使用DataFrame时，如果涉及转换操作，需要引入隐式转换规则
    //注意：这里不是包引入，而是对象的属性引入

    //:涉及到运算的时候, 每列都必须使用$, 或者采用引号表达式：单引号+字段名
//    df.select("username","age").show
//
//    df.select($"age"+1 as "newAge").show
//    df.select('age+1 as "newAge").show

    //DataSet
    val seq = Seq(1, 2, 3)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()


    //RDD <=> DataFrame
    val personRDD: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("zhangsan", 3), ("lisi", 3)))
    val df: DataFrame = personRDD.toDF("name", "id")
    val perRdd: RDD[Row] = df.rdd

    //DataFrame <=> DataSet
    val ds1: Dataset[Person] = df.as[Person]
    val df1: DataFrame = ds1.toDF()


    //RDD <=> DataSet
    val personRdd: RDD[Person] = spark.sparkContext.makeRDD(List(Person("zhangsan", 3), Person("lisi", 3)))
    val perDS: Dataset[Person] = personRdd.toDS()
    perDS.rdd
    //tudo关闭环境
    spark.close()

  }

  case class Person(name:String,age:Long)
}
