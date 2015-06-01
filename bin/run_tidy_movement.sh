#!/bin/bash
set -e

if [ $# -lt 2 ]; then
 echo "Usage: $0 <in> <out>"
 exit -1
fi

# parse command options
input=$1
output=$2_$(date +"%y%m%dT%H%M%S")
echo "Output: $output"

TARGET=$(find $(dirname $0) -name flowmap-assembly* | head -1)

spark-submit2 \
    --class cn.edu.sjtu.omnilab.flowmap.hz.TidyMovementJob \
    $TARGET $input $output

hadoop fs -tail $output/part-00000