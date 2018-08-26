# Weibo_UsersRecommend
使用Spark GraphX基于PageRank算法构建一个仿微博用户好友的分布式推荐系统。

项目介绍以及说明：

[仿微博用户推荐实现分布式推荐系统](https://blog.csdn.net/qq_37142346)

## 注意事项

1. 代码中文件的路径用户可以修改为自己数据所处的位置。
2. 需要启动hadoop集群，这里使用了hadoop2.5.0-cdh5.3.6。
3. 代码执行顺序：首先执行DataFormatUtil工具类进行数据清洗处理；然后执行CollectNeighborIds计算用户二级邻居；执行SortIdsByPageRank文件对用户二级邻居进行rank评分，并且进行排序；RecommendTop5User实现用户Top5好友的推荐。
4.系统目录下的数据：relation.txt与secondIds.txt为测试数据。userrelation.txt与usersecondIds.txt为系统开发所用的数据。还有部分数据需要存储在HDFS文件系统中，需要注意。
5.该项目使用Scala语言开发。
