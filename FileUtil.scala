package cn.just.shinelon.GraphX.PageRank

import java.io._

import scala.collection.immutable.HashSet
import scala.io.Source

class FileUtil {
  /**
    * 写文件
    * @param ids
    * @param path
    */
  def writeFile(ids:HashSet[Long],path:String): Unit ={
    val fileOutput=new OutputStreamWriter(new FileOutputStream(
      new File(path)))
    for (elem <- ids) {
      println(elem)
      //元素间用空格分割
      fileOutput.write(elem.toLong+" ")
    }
    fileOutput.close()
  }

  /**
    * 读文件
    * @param path
    * @return
    */
  def reade(path:String):HashSet[Long]={
    var ids=new HashSet[Long]
    val fileInput=new InputStreamReader(new FileInputStream(
      new File(path)
    ))
    var buf=new Array[Char](1024)
    var count=0
    var buffer=new StringBuffer()
    while((count=fileInput.read(buf)) != -1){
      buffer.append(buf,0,count)
    }
    fileInput.close()
    buffer.toString.split(" ").foreach(id => ids+=id.toLong)
    ids
  }

  /**
    * 读文件
    * @param path
    * @return
    */
  def readeFile(path:String):HashSet[Long]={
    var ids=new HashSet[Long]
    Source.fromFile(path).foreach(text=>{
      text.toString.split(" ").foreach(id => ids+=id.toLong)
    })
    ids
  }




}
