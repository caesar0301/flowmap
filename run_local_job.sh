set -e

rm -rf output

sbt assembly

spark-submit \
    --master local \
    --class sjtu.omnilab.kalin.hz.PrepareData \
    target/scala-2.10/kalin-assembly-0.1.0.jar \
    input/ \
    output/
