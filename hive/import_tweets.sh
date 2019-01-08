#!/usr/bin/env bash

hadoop=$HADOOP_HOME/bin/hadoop
hdfs=$HADOOP_HOME/bin/hdfs
hive=$HIVE_HOME/bin/hive

$hive -f ./create_tweets_raw.sql
$hadoop fs -mkdir /user/spark/streaming/twitter/processed

for currentpath in `$hadoop fs -ls /user/spark/streaming/twitter/ | awk '{print $NF}' | grep -v items | tr '\n' ' '`
do
    if `$hdfs dfs -test -e $currentpath/part-* >/dev/null`; then
        $hive -e "LOAD DATA INPATH \"${currentpath}\" INTO TABLE TWEET.TWEETS_RAW;"

        $hadoop fs -mv $currentpath /user/spark/streaming/twitter/processed/
    fi
done

