#!/usr/bin/env bash

hive=$HIVE_HOME/bin/hive

$hive -f ./load_tweets.sql
