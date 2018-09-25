#!/bin/bash

#Leave empty
export LD_LIBRARY_PATH=
export JAVA_LIBRARY_PATH=

bin=$(which $0)
bin=$(dirname ${bin})
bin=$(cd "$bin"; pwd)

classPath="${bin}"/../jars/*
mainClass="com.sebastian.fdx.executor/DataExchanger"
config="${bin}"/../conf/parameter.xml

CMD="java -cp .:${classPath} ${mainClass} ${config} $@"
echo "$CMD"
$CMD
