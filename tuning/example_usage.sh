#!/bin/bash

# Get server cpu and network consumption numbers
function utilizations {
while IFS='' read -r line || [[ -n "$line" ]]; do
    #echo "Text read from file: $line"
j=1
ssh -o ServerAliveInterval=60 $line 'mpstat -P ALL 50 4' > utils/server$j"_util$1.log" & 
ssh -o ServerAliveInterval=60 $line 'ifstat 10 22' > net_utils/server$j"_net$1.log" &
let j=j+1
done < hosts
}

# Get all the metrics provided by the Storm metrics UI
function getmetrics {
while IFS='' read -r line || [[ -n "$line" ]]; do
ssh -n -o ServerAliveInterval=60 $line "test -s $STORM_HOME/logs/metrics.log"
if [ $? -eq 0 ]; then
    mkdir -p logs
    scp -r $line:$STORM_HOME/logs/logs/metrics.log* logs/
    scp -r $line:$STORM_HOME/logs/metrics.log logs/
    ssh -n -o ServerAliveInterval=60 $line "rm -rf $STORM_HOME/logs"
    #mkdir -p logs/logs/
    #cp logs/metrics.log logs/logs/metrics.log
    ls -r logs/metrics* | xargs -I {} cat {} >> metrics/metrics$1.log
    rm -rf logs/
fi
done < hosts
}

function cleanup {
while IFS='' read -r line || [[ -n "$line" ]]; do
ssh -n -o ServerAliveInterval=60 $line "rm -rf $STORM_HOME/logs/metrics.log"
done < hosts
}

source environment 

#Kil any older running topologies
$STORM_HOME/bin/storm kill $TOPOLOGY -w 1
sleep 5

mkdir -p config_files
echo nfiles
mkdir -p utils
mkdir -p net_utils
mkdir -p perf
cleanup

max=3
retries=3

while true; do
# Start the benchmark
../bin/stormbench -storm $STORM_HOME/bin/storm -jar ../target/storm-benchmark-0.1.0-jar-with-dependencies.jar -conf ~/.storm/$CONF  storm.benchmark.tools.Runner storm.benchmark.benchmarks.$TOPOLOGY &
utilizations 0

#Start the T-Digest Server
kill -9 $(jps | grep "TServer" | awk '{print $1}')
nohup java -cp $TDIGEST_JAR com.tdigestserver.TServer $TSERVER_PORT &

sleep 20
end=$((SECONDS+200))
flag=true
count=0
while [ $SECONDS -lt $end ]; do
    # Do what you want.
string="$(ls reports/*.csv| tail -1 | xargs -I {} tail -1 {})"
if [[ $string == *",0,"* ]] || [[ $string == *"-"* ]]
then
  let count=count+1
  if [ "$count" -gt "$max" ] 
  then
     flag=false
     break;
  fi
fi
sleep 10
done

python storm_metrics.py 0

$STORM_HOME/bin/storm kill $TOPOLOGY -w 1 

sleep 20 

if [[ $flag ]]; then
  mkdir -p metrics
  copycounters 0

#Arguments: Directory, Index, Threads, number of nodes, number of spout, percentile latency, skip intervals, tolerance
  if python process.py json_files/ 0 90 3 3 99 10 1.1; then echo "Exit code of 0, success"; else continue; fi

  kill -9 $(jps | grep "TServer" | awk '{print $1}')
  break

else
  echo "Run failed" 
  if [ "$retries" -eq "0" ]
  then
    break
  else
    let retries=retries-1
    continue
  fi
fi

done
