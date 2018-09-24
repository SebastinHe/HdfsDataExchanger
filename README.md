# HdfsDataExchange(hdfs集群间数据交换)
## 
## 功能介绍：
  1. 不同版本的hdfs间文件迁移。
  2. 本地文件系统与hdfs间文件迁移。

## 原理说明:
* 抽象一套hdfs文件操作接口
* 使用不同版本的hadoop分别实现接口
* 使用jetty的WebAppClassLoader分别加载两套(源和目标)hdfs相关的jar，并创建对应的FileSystem
* 使用源FileSystem读取数据到缓冲区，然后使用目标FileSystem将缓冲区的数据写入到目标文件。

## 使用说明:
  
