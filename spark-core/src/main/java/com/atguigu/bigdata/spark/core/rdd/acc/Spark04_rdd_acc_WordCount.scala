package com.atguigu.bigdata.spark.core.rdd.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * @author wencheng
 * @create 2022/3/30 19:09
 *
 *  自定义累加器；分区之间共享写数据
 *  fore 分布式循环
 *
 */
object Spark04_rdd_acc_WordCount {

  def main(args: Array[String]): Unit = {
    //创建环境

    // local[*] 按照当前机器的所有的核心数  处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      "hello", "spark", "scala", "hello"
    ))

    //原来写法
    //    val reduceRdd: RDD[(String, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)
    //    reduceRdd.collect().foreach(println)

    //todo 自定义累加器
    val wcAcc = new AccWordCount()
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach((word: String) => {
      wcAcc.add(word)
    })

    println(wcAcc.value)

    //关闭环境
    sc.stop()
  }
}

/**
 * 1.继承 AccumulatorV2 定义参数反省
 * IN
 * OUT
 * 2.重写方法
 */
class AccWordCount extends AccumulatorV2[String, mutable.Map[String, Long]] {
  private val wcMap = mutable.Map[String, Long]()

  //判断是否是初始状态
  override def isZero: Boolean = {
    wcMap.isEmpty
  }

  //数据重置
  override def reset(): Unit = {
    wcMap.clear()
  }

  //获取累加器需要的词
  override def add(word: String): Unit = {
    val newCnt = wcMap.getOrElse(word, 0L) + 1
    wcMap.update(word, newCnt)
  }

  // Driver合并多个累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

    val map1 = this.wcMap
    val map2 = other.value

    map2.foreach{
      case ( word, count ) => {
        val newCount = map1.getOrElse(word, 0L) + count
        map1.update(word, newCount)
      }
    }
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new AccWordCount()
  }

  override def value: mutable.Map[String, Long] = {
    wcMap
  }
}