#!/bin/bash
set -e

SCALA_VERSION=2.10

if [ $# -lt 2 ]; then
 echo "Usage: local_test.sh <in> <out>"
 exit -1
fi

input=$1
output=$2
rm -rf output || echo "WARN: no $output ..."

sbt assembly
TARGET=$(find target/scala-$SCALA_VERSION/ -name *assembly* | head -1)

spark-submit \
    --master local \
    --class cn.edu.sjtu.omnilab.kalin.hz.SampleUsersJob \
    $TARGET $input $output
