#!/bin/bash

# Get rankings for all the metrics
for i in `seq 50 10 90` 
do
python ee.py lat_$i numbers.csv lat_$i.html
done

python ee.py throughput numbers.csv throughput.html
