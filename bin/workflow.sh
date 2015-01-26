#!/bin/bash
set -e

. /home/chenxm/.bashrc --source-only

function hadoop_rm () {
    hadoop fs -rm -r $@
}

INPUT_LOGS=/user/chenxm/HZLOGS/SET*
HZ_LOGS=/user/chenxm/hz_city_logs
TIDY_MOVE=/user/chenxm/hz_tidy_movement
SAMPLE_USERS=/user/chenxm/hz_sample_users

PACKAGE=cn.edu.sjtu.omnilab.kalin
TARGET=kalin-assembly-0.1.1.jar

hadoop_rm $HZ_LOGS || echo "Clean trash ..."
spark-submit2 --class $PACKAGE.hz.FilterDataGeo $TARGET $INPUT_LOGS $HZ_LOGS

hadoop_rm $TIDY_MOVE || echo "Clean trash ..."
spark-submit2 --class $PACKAGE.hz.TidyMovementJob $TARGET $HZ_LOGS $TIDY_MOVE

hadoop_rm $SAMPLE_USERS || echo "Clean trash ..."
spark-submit2 --class $PACKAGE.hz.SampleUsersJob $TARGET $TIDY_MOVE $SAMPLE_USERS