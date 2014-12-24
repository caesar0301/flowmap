set -e

rm -rf output

sbt package

spark-submit \
    --master local \
    --class sjtu.omnilab.kalin.hz.UserMovement \
    target/scala-2.10/kalin_2.10-0.1.0.jar \
    input \
    output
