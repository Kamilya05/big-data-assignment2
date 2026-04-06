#!/bin/bash

INPUT=${1:-/input/data}
OUTPUT=/indexer/index

hdfs dfs -rm -r -f $OUTPUT
chmod +x mapreduce/mapper1.py
chmod +x mapreduce/reducer1.py
chmod 744 mapreduce/mapper1.py
chmod 744 mapreduce/reducer1.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
  -input $INPUT \
  -output $OUTPUT \
  -mapper mapreduce/mapper1.py \
  -reducer mapreduce/reducer1.py \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py

echo "Index created in HDFS at $OUTPUT"