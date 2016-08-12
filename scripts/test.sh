#!/bin/bash
mkdir -p ~/perf
PID=$(jps | grep 'supervisor' | awk '{print $1}') 
echo $PID
perf stat -p $PID -a -I 100000 -o ~/perf/test.log & 
PERF_PID=$!
sleep 110
kill -9 $PERF_PID
