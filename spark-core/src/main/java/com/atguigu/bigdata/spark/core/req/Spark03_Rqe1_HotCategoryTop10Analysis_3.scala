package com.atguigu.bigdata.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author wencheng
 * @create 2022/5/3 9:37
 */
object Spark03_Rqe1_HotCategoryTop10Analysis_3 {

  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

    val sc = new SparkContext(sparkConf)


    //todo 热门产品排名
    //使用累加器统计

    //1.读取原始数据
    val actionRDD = sc.textFile("datas/user_action_data/user_visit_action.txt")

    val accHotCount = new AccHotCount()
    sc.register(accHotCount,"accHotCount")

    actionRDD.foreach(
      action => {
        val datas  = action.split("_")
        if (datas(6) != "-1") {
          accHotCount.add(UserAction(datas(6),0,0,0,0))
        } else if (datas(8) != "null") {
          val cids: Array[String] = datas(8).split(",")
          cids.map(
            cid =>   accHotCount.add(UserAction(cid,1,0,0,0))

          )
        } else if (datas(10) != "null") {
            val cids: Array[String] = datas(10).split(",")
            cids.map(
              cid =>   accHotCount.add(UserAction(cid,2,0,0,0))
            )
        }
      }
    )


    val sourceRDD: mutable.Map[String, UserAction] = accHotCount.value
    val userActions: mutable.Iterable[UserAction] = sourceRDD.map(_._2)


    val sort: List[UserAction] = userActions.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    )



    //6.将结果数据打印出来
    sort.take(10).foreach(println)


    sc.stop()

    val endTime: Long = System.currentTimeMillis()

    println(endTime - startTime)
  }

}

/**
 * 1.继承AccumulatorV2
 *  IN
 *  OUT
 * 2.重写方法
 */
class AccHotCount extends AccumulatorV2[UserAction,mutable.Map[String, UserAction]]{

  private val dataMap: mutable.Map[String, UserAction] = mutable.Map[String, UserAction]()

  override def isZero: Boolean = {
    dataMap.isEmpty
  }

  override def copy(): AccumulatorV2[UserAction,mutable.Map[String, UserAction]] = {
    new  AccHotCount()
  }

  override def reset(): Unit = {
    dataMap.clear()
  }

  override def add(v: UserAction): Unit = {
    val action: UserAction = dataMap.getOrElse(v.cid, v)
    action.actionType match {
      case 0 =>{
        action.clickCount +=1
      }
      case 1 =>{
        action.orderCount +=1
      }
      case 2=>{
        action.payCount +=1
      }
      case _ =>{
        action.clickCount +=1
      }
    }
    dataMap.update(action.cid,action)
  }

  override def merge(other: AccumulatorV2[UserAction,mutable.Map[String, UserAction]]): Unit = {
    val map1: mutable.Map[String, UserAction] = dataMap
    val map2: mutable.Map[String, UserAction] = other.value

    map2.foreach{
      case (k,v) =>{
        val action: UserAction = map1.getOrElse(k, UserAction(k, 0, 0, 0, 0))
        action.actionType = v.actionType
        action.clickCount+=v.clickCount
        action.orderCount+=v.orderCount
        action.payCount+=v.payCount
        map1.update(k,action)
      }
    }
  }

  override def value: mutable.Map[String, UserAction] = {
    dataMap
  }
}

//样例类在编译时，会自动混入序列化特质（实现可序列化接口）
case class UserAction(var cid:String,var actionType:Int,var clickCount:Int,var orderCount:Int,var payCount:Int)