package cn.just.shinelon.GraphX.PageRank

import org.apache.spark.graphx.{EdgeRDD, Graph, VertexRDD}

import scala.collection.immutable.HashSet

class GraphxUtil {

  /**
    * 根据二级邻居构造子图
    * @param graph
    * @param ids
    * @return
    */
  def getSubGraphxVertices(graph:Graph[Int,Int],ids:HashSet[Long]): VertexRDD[Long] ={
    val vertexRDD=graph.subgraph(vpred=(id,attr)=>ids.contains(id))
    vertexRDD.vertices.mapValues((_,attr)=>attr.toLong)
  }

  def getSubGraphxEdges(graph:Graph[Int,Int],ids: HashSet[Long]):EdgeRDD[Long]={
    val edgeRDD=graph.subgraph(vpred=(id,attr)=>ids.contains(id))
    edgeRDD.edges.mapValues(edge=>edge.attr.toLong)
  }

}
