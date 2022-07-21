package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
 * @author wencheng
 * @create 2022/3/24 10:04
 */
object Spark02_Word_Count {

  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和Spark框架的连接
    //类似mysql驱动的连接  JDBC:Connection

    //指定spark运行位置                          local模式
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作

    //1.读取文件数据  按行读取
    val line = sc.textFile("datas/input")

    //2.切分单词  扁平转换
//    val words = line.flatMap(_.split(" ")) //  匿名函数简化
    val words: RDD[String] = line.flatMap(splitWord)


    val hashMap = HashMap[String,Int]()

    words.foreach(word=>{
      if(hashMap.contains(word)){
        val maybeInteger :Int = hashMap.get(word).getOrElse(0)
        hashMap.put(word,maybeInteger+1)
      } else {
        hashMap.put(word,1)
      }
    })

    hashMap.foreach{
      case (k,v) => println(k + "->" + v)
    }

    //3.相同单词分组
    val wordGroup = words.groupBy(word => word)

    //4.聚合统计单词计数   hello 2  scala  4
    val wordCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array = wordCount.collect()
    //打印结果
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }

  def splitWord(line: String): Array[String] ={
    line.split(" ")
  }

}
