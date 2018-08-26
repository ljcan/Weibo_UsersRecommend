package cn.just.shinelon.GraphX.PageRank

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.derby.iapi.services.io.ArrayOutputStream
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object CollectNeighborIds {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf()
          .setMaster("local")
          .setAppName("CollectNeighborIds")

    val sc=new SparkContext(conf)

    //构造图
    val graph=GraphLoader.edgeListFile(sc,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\relation.txt")
//    val graph=GraphLoader.edgeListFile(sc,"hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/relation.txt")

    val graphNeighborUtil=new GraphNeighborUtil

//    val set=graphNeighborUtil.getFristNeighborIds(3,graph)
//
//    set.foreach(println)

    //获取二级邻居的ids
    val secondIds=graphNeighborUtil.getIds(1000080335,graph)


    //***********************测试数据集*********************************
//    if(secondIds.isEmpty){println("为空")}else{println("不为空")}

    //封装为工具类
//    val fileOutput=new OutputStreamWriter(new FileOutputStream(
//      new File("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\secondIds.txt")))
//    for (elem <- secondIds) {
//      println(elem)
//     fileOutput.write(elem.toString+" ")
//    }
//    fileOutput.close()

    //****************************************************************
//    println(secondIds.size)
    val fileUtil=new FileUtil
    fileUtil.writeFile(secondIds,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\usersecondIds.txt")
//    fileUtil.writeFile(secondIds,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\hanhan_usersecondIds.txt")


  }

}
