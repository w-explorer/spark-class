package com.atguigu.bigdata.spark.streaming

import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wencheng
 * @create 2022/8/17 19:18
 */
object SparkStreaming03_DIY {

  /**
   * 自定义数据源接收器
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val message: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    message.print()
    ssc.start()
    ssc.awaitTermination()

  }

  /*
    自定义数据采集器
    1. 继承Receiver，定义泛型, 传递参数
    2. 重写方法
     */

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag){
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()

    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
