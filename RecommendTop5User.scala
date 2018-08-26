package cn.just.shinelon.GraphX.PageRank

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 为1000080335用户推荐top5好友（按照PageRank排序）
  */
object RecommendTop5User {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local[8]")
          .setAppName("RecommendTop5User")

    val sc=new SparkContext(conf)

    sc.textFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/userrank.txt").take(5).foreach(println)

//    sc.textFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/graphx/hanhan_userrank.txt").take(5).foreach(println)

    //result
    /**
      * (userId,rank)
      * (1618051664,59.89345814196924)
        (1191258123,54.934897577144696)       //韩寒
        (2656274875,54.37123848880913)
        (1496852380,52.85206155862678)
        (1761179351,47.46940913885135)
      */

  }

}
