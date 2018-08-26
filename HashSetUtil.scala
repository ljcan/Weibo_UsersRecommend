package cn.just.shinelon.GraphX.PageRank



import scala.collection.immutable.HashSet

/**
  * 防止在获取好友的好友的id时为好友的Id，进行筛选操作
  * @author shinelon
  */
class HashSetUtil[Long] {
  /**
    * 删除两个集合之中重复的元素
    * @param secondIds
    * @param firstIds
    * @return
    */
  def removeRepeate(secondIds:HashSet[Long],firstIds: HashSet[Long]):HashSet[Long]={
    secondIds.filter(id=>firstIds.contains(id)==false)
  }

}
