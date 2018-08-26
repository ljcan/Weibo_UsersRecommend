package cn.just.shinelon.GraphX.PageRank

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * The `App` trait can be used to quickly turn objects
  *  into executable programs.
  */
object SortIdsByPageRank extends App{
  val conf=new SparkConf()
        .setMaster("local[8]")
        .setAppName("SortIdsByPageRank")
  val sc=new SparkContext(conf)

  val graphNeighborUtil=new GraphNeighborUtil

  var graph=GraphLoader.edgeListFile(sc,"hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/relation.txt")


  //*****************************测试数据**************************

//  val fileUtil=new FileUtil
  //读取二级邻居ids
//  val ids:HashSet[Long]=fileUtil.readeFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\usersecondIds.txt")
//
//  if(ids.isEmpty){
//    println("二级邻居为空")
//  }else{
//    println("不为空")
//    ids.take(10).foreach(println)
//  }

  //*************************************************************

  var ids=new HashSet[Long]
  sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\usersecondIds.txt")
    .flatMap(_.split(" ")).foreach(id=>{
     ids += id.toLong
  })
  //韩寒
//  sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\hanhan_usersecondIds.txt")
//    .flatMap(_.split(" ")).foreach(id=>{
//    ids += id.toLong
//  })

    if(ids.isEmpty){
      println("二级邻居为空")
    }else{
      println("不为空")
      ids.take(10).foreach(println)
    }


  //构建ids图
  val graphxUtil=new GraphxUtil
//  val subgraph=graph.subgraph(vpred=(id,attr)=>(id.toLong,attr.toLong)!=null)
  val vertices=graphxUtil.getSubGraphxVertices(graph,ids)
  val edges=graphxUtil.getSubGraphxEdges(graph,ids)

  val subgraph=Graph(vertices,edges)

  val firstNeighbor:VertexRDD[Double]=subgraph.pageRank(0.01).vertices

  val neighborRank = firstNeighbor.filter(pred=>{
    var flag=false
    ids.foreach(id=>if(id == pred._1) flag = true)
    flag
  }).sortBy(x=>x._2,false)          //按照rank从大到小排序
    .coalesce(1)
    .saveAsTextFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/userrank.txt")
//    .saveAsTextFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/hanhan_userrank.txt")

  sc.stop()


}
