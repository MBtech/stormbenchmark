#!/bin/bash
cycles=0
inst=0
for i in perf/test$1_*.log; 
do
x=$(cat $i | grep -E "cycles\s" | awk '{print $2}' |sed -r 's/,//g')
y=$(cat $i | grep -E "instructions\s" | awk '{print $2}' |sed -r 's/,//g')

cycles=$((x + cycles))
inst=$((y + inst))
done

python ipc.py $cycles $inst
