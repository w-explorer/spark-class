package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wencheng
 * @create 2022/8/17 19:18
 */
object SparkStreaming01_WordCount {
  /**
   * 使用netcat 工具 向9999 不断发送数据，监听端口 9999 的数据 并进行wordCount 操作
   *
   * cmd  nc -lp 999
   * @param args
   */

  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //todo 业务逻辑

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = words.map((_, 1))
    val wordCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordCount.print()



    //todo 关闭环境对象
//    ssc.stop()

    //由于sparkStreaming 是长期执行的任务，不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束。所以也不能让main方法执行完毕

    //1.启动采集器
    ssc.start()

    //2.等待采集器的关闭
    ssc.awaitTermination()

  }

}
