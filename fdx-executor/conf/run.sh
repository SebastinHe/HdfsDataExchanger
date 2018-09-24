#!/bin/bash

export LD_LIBRARY_PATH=
export JAVA_LIBRARY_PATH=

classPath="/usr/local/complat/hdfs-dataexchange-sam/main/*"
mainClass="com.xunlei.bigdata.dataexchange.Main"
config="/usr/local/complat/hdfs-dataexchange-sam/jobs/hdfs1-hdfs1/parameter.xml"

CMD="java -cp .:${classPath} ${mainClass} ${config} $@"
echo "$CMD"
$CMD