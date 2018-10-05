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
  1. h1为hadoop1的配置(conf)和相关的jar包(jars)
  2. h2为hadoop2的配置(conf)和相关的jar包(jars)
  3. main为主程序相关的配置(conf)、相关的jar包(jars)和启动脚本(bin)
* 配置说明
  1. h1和h2的jars目录下应该包含依赖的所有hadoop jar包，当前只包含了fdx-hadoopX-filesystem-1.0-SNAPSHOT.jar,用户在使用时需根据实际的hadoop版本自行编译，并将依赖的jar和编译出来的jar一并放到对应的目录下。
  2. main/jars目录放fdx-executor和fdx-filesystem-api编译出的jar以及依赖的所有jar。
  3. main/conf/parameter.xml
  <table>
  <tr>
    <th>参数名</th>
    <th>说明</th>
  </tr>
  <tr>
    <td>thread.count</td>
    <td>并行进行迁移数据的线程数量</td>
  </tr>
  <tr>
    <td>buffer.size</td>
    <td>读取数据缓冲区大小</td>
  </tr>
  <tr>
    <td>src/dest.hdfs.resource.path</td>
    <td>源/目标hdfs的jar包目录</td>
  </tr>
  <tr>
    <td>src/dest.filesystem.implement</td>
    <td>源/目标针对抽象出的hdfs接口(com.sebastian.fdx.fs.api.BaseFileSystem)的实现类，WebAppClassLoader根据此配置加载对应的实现</td>
  </tr>
  <tr>
    <td>src/dest.hdfs.conf.path</td>
    <td>源/目标对应的core-site.xml和hdfs-site.xml</td>
  </tr>
</table>
