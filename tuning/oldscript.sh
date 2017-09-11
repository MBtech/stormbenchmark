#!/bin/bash

. parse_yaml.sh

# read yaml file

# access yaml content

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

STORM_HOME=~/bilal/storm
#STORM_HOME=~/ansible-test/storm/apache-storm-0.10.0

mkdir -p config_files
python wspOld.py
i=0
mkdir -p utils
mkdir -p net_utils
nfiles=$(ls config_files/ | wc -l)
echo $nfiles
cleanup
for j in `seq 1 $nfiles`; do
eval $(parse_yaml config_files/test$i.yaml "config_")
cp config_files/test$i.yaml ~/.storm/rollingcount.yaml
#cat ~/.storm/sol.yaml
../bin/stormbench -storm $STORM_HOME/bin/storm -jar ../target/storm-benchmark-0.1.0-jar-with-dependencies.jar -conf ~/.storm/rollingcount.yaml  storm.benchmark.tools.Runner storm.benchmark.benchmarks.RollingCount &
utilizations $i
sleep 210
python storm_metrics.py $i
$STORM_HOME/bin/storm kill RollingCount -w 1
sleep 20

mkdir -p metrics
getmetrics
mkdir -p logs/logs/
cp logs/metrics.log logs/logs/metrics.log
ls -r logs/logs/metrics* | xargs -I {} cat {} >> metrics/metrics$i.log
python process.py json_files/ $i 94 3 0 99 10 1.1
let i=i+1
rm -rf logs/*
cleanup
done
#tar -czf test2_45k_15spout.tar.gz config_files/ json_files/ net_utils/ reports/ utils/ metrics/ numbers.csv
#rm -rf metrics config_files/ json_files/ net_utils/ reports/ utils/ numbers.csv logs
