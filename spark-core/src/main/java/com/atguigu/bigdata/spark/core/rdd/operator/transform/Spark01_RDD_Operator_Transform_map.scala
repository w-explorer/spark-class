package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark01_RDD_Operator_Transform_map {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    def mapDef (num :Int): Int ={
      num*2
    }

//    val mapRdd: RDD[Int] = rdd.map(mapDef)

    //匿名函数 不用定义一个方法
//    val mapRdd: RDD[Int] = rdd.map((num:Int)=>{num*2})

    //简略原则：当返回值只有一行时 可以省略大括号
//    val mapRdd: RDD[Int] = rdd.map((num:Int)=>{num*2})

    //当能自动推断类型时 可以省略类型
//    val mapRdd: RDD[Int] = rdd.map((num)=>num*2)
    //当参数列表只有一个 可以省略参数列表的小括号
//    val mapRdd: RDD[Int] = rdd.map(num=>num*2)

    //当参数执行过程中只出现一次  可以省略不写用 _代替
    val mapRdd: RDD[Int] = rdd.map(_*2)

    /**
     * map 将处理的数据进行逐条映射转换，这里的转换可以是类型的转换，也可以是值的转换
     */

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
