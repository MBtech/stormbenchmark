#!/bin/bash

function utilizations {
while IFS='' read -r line || [[ -n "$line" ]]; do
    #echo "Text read from file: $line"
j=1
ssh -o ServerAliveInterval=60 $line 'mpstat -P ALL 50 4' > utils/server$j"_util$1.log" & 
ssh -o ServerAliveInterval=60 $line 'ifstat 10 22' > net_utils/server$j"_net$1.log" &
let j=j+1
done < hosts
}
function redis_getmetrics {
$REDIS_HOME/./redis-cli keys "*" | sort -n | xargs -I {} $REDIS_HOME/./redis-cli get {} > metrics/metrics$1.log
}

function redis_cleanup {
$REDIS_HOME/./redis-cli FLUSHALL
}

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
done < $SCRIPT_HOME/hosts
}

function cleanup {
while IFS='' read -r line || [[ -n "$line" ]]; do
ssh -n -o ServerAliveInterval=60 $line "rm -rf $STORM_HOME/logs/metrics.log"
done < $SCRIPT_HOME/hosts
}
function getcounters {
sleep 40
while IFS='' read -r line || [[ -n "$line" ]]; do
scp -r test.sh $line:~/bilal/
ssh -f -n -o ServerAliveInterval=60 $line "bash ~/bilal/test.sh"
#ssh -n -o ServerAliveInterval=60 $line "PID=$(jps | grep 'worker' | awk '{print $1}'); perf stat -p $PID -a -I 300000 -o perf/test$1.log & PERF_PID=$!; sleep 310; kill -9 $PERF_PID" &
done < $SCRIPT_HOME/hosts
}

function copycounters {
while IFS='' read -r line || [[ -n "$line" ]]; do
scp -r $line:~/perf/test.log perf/test$1_$line.log
done < $SCRIPT_HOME/hosts
}

function queuestats {
while IFS='' read -r line || [[ -n "$line" ]]; do
# This should be ssh command
ssh -o ServerAliveInterval=60 ubuntu@sys-n03.info.ucl.ac.be "cd $STORM_HOME/logs/workers-artifacts; find $(ls -tr | tail -n1) -name 'worker.log.metrics' | xargs -I {} cat {} | grep -o 'population=[0-9]*'"
done < $SCRIPT_HOME/hosts
}
#python wspAlgorithm.py single 21
#STORM_HOME=~/bilal/storm
#STORM_HOME=/usr/local/ansible-test/storm/apache-storm-0.9.3
#STORM_HOME=~/ansible-test/storm/apache-storm-0.9.5
STORM_HOME=~/ansible-test/storm/apache-storm-1.0.1
REDIS_HOME=~/bilal/redis-3.2.0/src
SCRIPT_HOME=~/bilal/storm-benchmark/scripts
TOPOLOGY=RollingCount
CONF=rollingcount.yaml

#Kil any older running topologies
$STORM_HOME/bin/storm kill $TOPOLOGY -w 1
sleep 5

mkdir -p config_files
i=$1
#nfiles=$(ls config_files/ | wc -l)
mkdir -p utils
mkdir -p net_utils
mkdir -p perf
cleanup
max=3
retries=3
while true; do
#python randomize.py
cp config_files/test$i.yaml ~/.storm/$CONF
#cat ~/.storm/sol.yaml
$SCRIPT_HOME/../bin/stormbench -storm $STORM_HOME/bin/storm -jar $SCRIPT_HOME/../target/storm-benchmark-0.1.0-jar-with-dependencies.jar -conf ~/.storm/$CONF  storm.benchmark.tools.Runner storm.benchmark.benchmarks.$TOPOLOGY &
utilizations $i
kill -9 $(jps | grep "TServer" | awk '{print $1}')
nohup java -cp ~/bilal/TDigestService/target/TDigestService-1.0-SNAPSHOT-jar-with-dependencies.jar com.tdigestserver.TServer 11111 &
sleep 10
getcounters $i &
#PID=$(jps | grep "worker" | awk '{print $1}')
#perf stat -p $PID -a -I 200000 -o perf/test$i.log &
#PERF_PID=$!
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
#sleep 200
#kill -9 $PERF_PID

#sleep 210
python $SCRIPT_HOME/storm_metrics.py $i
$STORM_HOME/bin/storm kill $TOPOLOGY -w 1 
sleep 20 

if [[ $flag ]]; then
mkdir -p metrics
copycounters $i
#getmetrics $i

#redis_getmetrics $i

#mkdir -p logs/logs/
#cp logs/metrics.log logs/logs/metrics.log
#ls -r logs/logs/metrics* | xargs -I {} cat {} >> metrics/metrics$i.log
#rm -rf logs
echo "Current iteration number is $i"
#Arguments: Directory, Index, Threads, number of nodes, number of spout, percentile latency, skip intervals, tolerance
if python $SCRIPT_HOME/process.py json_files/ $i 90 3 3 99 10 1.1; then echo "Exit code of 0, success"; else continue; fi
#python process.py json_files/ $i 90 3 3 99 10 1.1
kill -9 $(jps | grep "TServer" | awk '{print $1}')
break
#let i=i+1
#nfiles=$(ls config_files/ | wc -l)
#for j in `seq $i $nfiles`; do    count=$(cat numbers.csv | grep "^$j," | wc -l); if [ $count -eq 0 ]; then let i=j; break; fi; done
#echo "Next iteration number is $i"
#let i=i+1
#if [ "$i" -eq "$nfiles" ]
#	then
#redis_cleanup	
#break
#fi
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
#cleanup
#redis_cleanup
done
#tar -czf test_72k.tar.gz config_files/ json_files/ net_utils/ reports/ utils/ metrics/ numbers.csv
