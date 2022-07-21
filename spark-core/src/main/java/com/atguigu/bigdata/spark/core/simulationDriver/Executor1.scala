package com.atguigu.bigdata.spark.core.simulationDriver

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}


/**
 * @author wencheng
 * @create 2022/3/29 10:23
 */
object Executor1 {

  def main(args: Array[String]): Unit = {
    //服务端 创建服务端
    val socketServer = new ServerSocket(8888)

    println("服务端已启动，等待接收连接")

    //等待连接  线程阻塞
    val client: Socket = socketServer.accept()

    val in: InputStream = client.getInputStream

    val objectInputStream = new ObjectInputStream(in)

//    val i: Int = in.read()

    val task: Task = objectInputStream.readObject().asInstanceOf[Task]
    println("接收端计算数据："+task.compute())

    objectInputStream.close()
    client.close()
    socketServer.close()

  }
}
