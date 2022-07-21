package com.atguigu.bigdata.spark.core.simulationDriver

/**
 * @author wencheng
 * @create 2022/3/29 11:13
 */
class Task extends Serializable {

  val data = List(1,2,3,4)

//  var logic = (x : Int)=>{ x*2}

//  var logic :Int =>Int= _*2

  def logic(num:Int) ={
    num*2
  }


  def compute() ={
    data.map(logic)
  }
}
