#!/bin/bash
set -e

APPNAM="cn.edu.sjtu.omnilab.kalin.cc.GenFlowmap"
BINJAR="$(dirname $0)/$(find $(dirname $0) -name kalin-assembly* | head -1)"

if [ $# -lt 2 ]; then
 echo "Usage: $0 <in> <out> [interval] [minnum]"
 exit -1
fi

# parse command options
input=$1
output=$2_$(date +"%y%m%dT%H%M%S")
echo "Output: $output"

spark-submit2 --class $APPNAM $BINJAR $input $output

hadoop fs -tail $output/part-00000
