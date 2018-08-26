package cn.just.shinelon.GraphX.PageRank

import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.collection.immutable.HashSet


/**
  * @author shinelon
  */
class GraphNeighborUtil {
  /**
    * 根据id得到其好友的id
    * 调用graph的aggregateMessages方法收集一级邻居
    * @param id
    * @param graph
    * @return
    */
  def getFristNeighborIds(id:Long,graph:Graph[Int,Int]):HashSet[Long]={
    //aggregateMessages[Int]发送给每条边的每个顶点Int类型的消息
    val firstNeighbor:VertexRDD[Int]=graph.aggregateMessages[Int](triplet=>{
      if(triplet.srcId==id){
        triplet.sendToDst(1)
      }
    },
      (a,b)=>b+1)     //聚合相同顶点接收到的消息

//    firstNeighbor.foreach(println)
    var fristIds=new HashSet[Long]()
    firstNeighbor.collect().foreach(a=>fristIds+=a._1)
    fristIds
  }

  /**
    * 通过用户id的集合得到好友的id集合
    * @param firstIds
    * @param graph
    * @return
    */
  def getSecondNeighborIds(firstIds:HashSet[Long] , graph:Graph[Int,Int]):HashSet[Long]={
    var secondIds=new HashSet[Long]()
    firstIds.foreach(id=>{
      val secondNeighbors=getFristNeighborIds(id,graph)
      secondNeighbors.foreach(secondId=>secondIds+=secondId)
//      secondIds.foreach(println)
    })
//    println("调用了")
//    secondIds.foreach(println)

    //防止在获取好友的好友的id时为好友的Id，进行筛选操作
    val hashSetUtil=new HashSetUtil[Long]
    hashSetUtil.removeRepeate(secondIds,firstIds)
  }

  /**
    * 根据用户的id得到好友的好友的信息
    * @param id
    * @param graph
    * @return
    */
  def getIds(id:Long,graph:Graph[Int,Int]):HashSet[Long]={
      getSecondNeighborIds(getFristNeighborIds(id,graph),graph)
  }

}
