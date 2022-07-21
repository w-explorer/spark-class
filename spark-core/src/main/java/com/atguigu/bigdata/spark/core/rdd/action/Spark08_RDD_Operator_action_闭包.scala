package com.atguigu.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wencheng
 * @create 2022/4/1 0:00
 */
object Spark08_RDD_Operator_action_闭包 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)


    //todo 创建RDD 算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val user = User()

    //org.apache.spark.SparkException: Task not serializable
    //java.io.NotSerializableException: com.atguigu.bigdata.spark.core.rdd.action.Spark08_RDD_Operator_action_闭包$User

    //RDD算子中传递的函数是会包含闭包操作，那么就会进行检查功能、

    //driver 把算子之外的数据需要传给执行器  excuter 网络之间传输需要序列化
    rdd.foreach(num => {
      println(user.age+num)
    })

    sc.stop()
  }


//  class User extends Serializable {
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
   case class User (){
    var age: Int = 30
  }
}
