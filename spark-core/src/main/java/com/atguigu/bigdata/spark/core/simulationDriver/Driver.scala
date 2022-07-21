package com.atguigu.bigdata.spark.core.simulationDriver

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author wencheng
 * @create 2022/3/29 10:23
 */
object Driver {

  def main(args: Array[String]): Unit = {

    //连接服务器

    val client = new Socket("localhost", 9999)

    //获取输出流

    val out: OutputStream = client.getOutputStream

    val outputStream = new ObjectOutputStream(out)

    val task = new Task()
    outputStream.writeObject(task)

    outputStream.flush()

    outputStream.close()
    client.close()
  }
}
