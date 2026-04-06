#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)

hdfs dfs -rm -r -f /user/root
hdfs dfs -mkdir -p /user/root
hdfs dfs -put -f a.parquet

spark-submit prepare_data.py

echo "DONE"