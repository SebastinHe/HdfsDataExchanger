# HdfsDataExchanger(hdfs集群间数据交换)
## 
## 功能介绍：
  1. 不同版本的hdfs间文件迁移。
  2. 本地文件系统与hdfs间文件迁移。

## 原理说明:
* 抽象一套hdfs文件操作接口。
* 使用源集群和目标集群对应版本的hadoop分别实现接口。
* 使用jetty的WebAppClassLoader分别加载两套(源和目标)hdfs相关的jar，并创建对应的FileSystem。
* 使用源FileSystem读取数据到缓冲区，然后使用目标FileSystem将缓冲区的数据写入到目标文件。

## 使用说明:
* data-exchanger目录下包含完整的部署目录
  1. h1为hadoop1的配置(conf)和先关的jar包(jars)
  2. h2为hadoop2的配置(conf)和相关的jar包(jars)
  3. main为主程序相关的配置(conf)、相关的jar包(jars)和启动脚本(bin)

  
