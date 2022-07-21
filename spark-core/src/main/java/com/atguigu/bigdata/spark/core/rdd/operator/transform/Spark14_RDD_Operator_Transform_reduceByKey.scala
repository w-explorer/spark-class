package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark14_RDD_Operator_Transform_reduceByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子   key-value 算子
    val rdd = sc.makeRDD(List(("a",1),("b",1),("b",2)))

    /**
     * reduceByKey:
     * 将数据按照指定的Partitioner 重新分区。并且按照key进行数据的分组聚合。
     *
     * 将相同的key 的value  进行聚合操作。
     */

//    rdd.reduceByKey(_+_).collect().foreach(println)
    rdd.reduceByKey((x,y)=>x+y).collect().foreach(println)
    rdd.reduceByKey(_+_,2).collect().foreach(println)

    sc.stop()
  }

}
