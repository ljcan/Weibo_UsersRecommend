package cn.just.shinelon.GraphX.PageRank

import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 数据清洗和转换操作
  */
object DataFormatUtil {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("DataFormatUtil")
          .setMaster("local[8]")

    val sc=new SparkContext(conf)

//    var list=new mutable.LinkedList[Tuple2[String,String]]()
    sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\GraphX\\PageRank\\userrelation.txt")
        .map(line=>{
          val elems=line.split(",")
          elems(0)+","+elems(1)+" "+elems(2)+","+elems(0)
        })
        .flatMap(_.split(" "))
        .map(str=>{
          val x=str.split(",")
          val userId=x(0)
          val friendId=x(1)
          userId+" "+friendId})
        .distinct()
//        .take(10)
//        .foreach(println)
        .saveAsTextFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/relation.txt")

  }

}
