package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark07_RDD_Operator_Transform_sample {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd = sc.makeRDD(List(1,2,3,4),1)


    /**
     * sample: 根据指定得规则从数据集中抽取数据
     *
     * 参数一withReplacement：表示抽取数据后是否将数据放回，true 放回，false 不放回
     * 参数二fraction：数据集中每一条数据被抽取到得概率
     * 参数三seed：抽取数据的随机算法因子
     *
     * 思考一个问题：有什么用
     *
     * 探查数据倾斜
     * 比如相同的key都放在一个组中，抽取数据进行判断
     *
     */


    val sampleRdd: RDD[Int] = rdd.sample(false, 0.25,1)
    sampleRdd.collect().foreach(println)

    sc.stop()
  }

}
