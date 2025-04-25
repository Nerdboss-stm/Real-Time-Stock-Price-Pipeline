#!/bin/bash

/opt/homebrew/Cellar/apache-spark/3.5.5/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  processing/spark_stream_consumer.py

