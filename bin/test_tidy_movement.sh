#!/bin/bash
set -e
SCALA_VERSION="2.10"

if [ $# -lt 2 ]; then
 echo "Usage: $0 <in> <out>"
 exit -1
fi

# parse command options
input=$1
output=$2_$(date +"%y%m%dT%H%M%S")
echo "Output: $output"

# build and run job
sbt -java-home $(/usr/libexec/java_home -v '1.6*') assembly
TARGET=$(find target/scala-$SCALA_VERSION/ -name *assembly* | head -1)

spark-submit \
    --master local \
    --class cn.edu.sjtu.omnilab.flowmap.hz.TidyMovementJob \
    $TARGET $input $output

# check out results
if [ -d $output ]; then
    echo "Output: "
    head -n 20 $output/part-00000
    echo "..."
fi