#!/bin/bash

QUERY=$1

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$QUERY"