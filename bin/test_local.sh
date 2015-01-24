#!/bin/bash

set -e

if [ $# -lt 2 ]; then
 echo "Usage: local_test.sh <in> <out>"
 exit -1
fi

input=$1
output=$2

rm -rf output || echo "WARN: no $output ..."

sbt package

spark-submit \
    --master local \
    --class cn.edu.sjtu.omnilab.kalin.hz.TidyMovementJob \
    target/scala-2.10/kalin_2.10-0.1.1.jar \
    $input \
    $output
