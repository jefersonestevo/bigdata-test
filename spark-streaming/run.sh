#!/usr/bin/env bash

cd ..
mvn clean install -DskipTests
cd spark-streaming

$SPARK_HOME/bin/spark-submit --class "br.com.test.bigdata.spark.streaming.SparkStreamingReader" --master local[4] ./target/*-with-dependencies.jar
