import yaml
import sys
import random
import os
import json
import csv
from randomize import select_write
from os.path import exists
from itertools import islice
import numpy
import shelve
import glob

def tail_latency(i,spout_num,percentile,nskip):
    tuple_latencies = list()
    split_latencies = list()
    count = 0
    skipcount = 0
    with open('metrics/'+'metrics'+i+'.log') as metric_file:
        for line in metric_file:
            if skipcount>nskip*spout_num:
                s = line
		split_latencies.append(s[s.find("[")+1:s.find("]")].replace(" ","").split(','))
                tuple_latencies.extend(s[s.find("[")+1:s.find("]")].replace(" ","").split(','))
                count+=1
                #print line
            else:
                skipcount+=1
                #print line
    #print count
    #print skipcount
    #print len(tuple_latencies)
    latencies = list()
    interval_latencies = [[]]
    #interval_latencies.insert(0,list())
    count = 0
    index = 0
    #print len(split_latencies)
    for latency in split_latencies:
        #print len(latency)
        lat = list()
        for l in latency:
            if not l == '':
                lat.append(l)
        if count == 10*spout_num:
            count = 0
            index = index + 1
            interval_latencies.append(list(lat))
        else:
            interval_latencies[index].extend(list(lat))
            count = count + 1 
    #print len(interval_latencies)
    #print interval_latencies[0]
    for latency in interval_latencies:
        print numpy.percentile(map(int,latency),percentile)
    print "Next run"    
    #for latency in tuple_latencies:
    #	if not latency =='':
    #        latencies.append(latency)
    #print len(latencies)
    #if len(latencies)>0:
        #median= numpy.percentile(map(int,latencies),50)
        #print numpy.percentile(map(int,latencies),percentile)
        #return median,numpy.percentile(map(int,latencies),90),numpy.percentile(map(int,latencies),percentile)
    #else:
    #    return float("inf"),float("inf"),float("inf")

for i in range(9,14):
    tail_latency(str(i),15,99,10)
