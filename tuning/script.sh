#!/bin/bash

function utilizations {
while IFS='' read -r line || [[ -n "$line" ]]; do
    #echo "Text read from file: $line"
j=1
ssh -o ServerAliveInterval=60 $line 'mpstat -P ALL 50 4' > utils/server$j_util$1.log &
ssh -o ServerAliveInterval=60 $line 'ifstat 10 22' > net_utils/server$j_net$1.log &
let j=j+1
done < hosts
}

function getmetrics {
while IFS='' read -r line || [[ -n "$line" ]]; do
ssh -n -o ServerAliveInterval=60 $line "test -s $STORM_HOME/logs/metrics.log"
if [ $? -eq 0 ]; then
    scp -r $line:$STORM_HOME/logs .
    ssh -n -o ServerAliveInterval=60 $line "rm -rf $STORM_HOME/logs"
fi
done < hosts
}

function cleanup {
while IFS='' read -r line || [[ -n "$line" ]]; do
ssh -n -o ServerAliveInterval=60 $line "rm -rf $STORM_HOME/logs/metrics.log"
done < hosts
}

#STORM_HOME=~/bilal/storm
STORM_HOME=~/ansible-test/storm/apache-storm-0.9.5

python wspAlgorithm.py single 30
i=0
echo $nfiles
mkdir -p utils
mkdir -p net_utils
cleanup
tries=0
while true; do
cp config_files/test$i.yaml ~/.storm/rollingcount.yaml
#cat ~/.storm/sol.yaml
../bin/stormbench -storm $STORM_HOME/bin/storm -jar ../target/storm-benchmark-0.1.0-jar-with-dependencies.jar -conf ~/.storm/rollingcount.yaml  storm.benchmark.tools.Runner storm.benchmark.benchmarks.RollingCount &
utilizations $i
sleep 30
end=$((SECONDS+200))
flag=true
echo "Start processing the tail"
while [ $SECONDS -lt $end ]; do
    # Do what you want.
sleep 5
string="$(ls reports/*.csv| tail -1 | xargs -I {} tail -1 {})"
if ( [[ $string == *",0,"* ]] || [[ $string == *"-"* ]] )  && (( "$i" != 1 )) ;
then
  let tries=tries+1
  if [ "$tries" -lt 4 ]
  then
  flag=false;
  break;
  fi
fi
done

#sleep 210
python storm_metrics.py $i
$STORM_HOME/bin/storm kill RollingCount -w 1
sleep 20

if $flag ; then
let tries=0
mkdir -p metrics
getmetrics
mkdir -p logs/logs/
cp logs/metrics.log logs/logs/metrics.log
ls -r logs/logs/metrics* | xargs -I {} cat {} >> metrics/metrics$i.log
python process.py json_files/ $i 64 3 30 99 10 1.1
let i=i+1
echo $i
nfiles=$(ls config_files/ | wc -l)
print $nfiles
if [ "$i" -eq "$nfiles" ]
	then
	break
fi
fi
rm -rf logs/*
cleanup
done
tar -czf test1_45k_9spout.tar.gz config_files/ json_files/ net_utils/ reports/ utils/ metrics/ numbers.csv
rm -rf metrics config_files/ json_files/ net_utils/ reports/ utils/ numbers.csv logs
