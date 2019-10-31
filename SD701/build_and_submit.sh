#!/bin/bash

# This script needs to be located where the build.sbt file is.
# It takes as optional parameter the path of the spark directory. If no path is specified
# it will look for the spark directory in $HOME.
#
# Come from INF729 TPs, 
#
# Paramters:
#  - $1 : job to execute
set -o pipefail

echo -e "\n --- building .jar --- \n"

sbt assembly || { echo 'Build failed' ; exit 1; }

echo -e "\n --- spark-submit --- \n"

path_to_spark="/infres/ir510/hadoop/spark-2.4.4-bin-without-hadoop/"


if [ -n "$2" ]; then path_to_spark=$2; fi

nohup $path_to_spark/bin/spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 10g --class paristech.$1 target/scala-2.11/*.jar >$1.log 2>&1
