package com.atguigu.bigdata.spark.core.simulationDriver

/**
 * @author wencheng
 * @create 2022/3/29 11:13
 */
class SubTask extends Serializable {

  var data : List[Int] = _

  var logic : Int =>Int = _


  def compute() ={
    data.map(logic)
  }

}
