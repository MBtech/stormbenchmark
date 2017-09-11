import yaml
import sys
import random
import os
import json
import csv
from os.path import exists
from utils import *
from itertools import islice
import numpy
import shelve
import glob
from operator import iadd
from tdigest import TDigest

def tail_latency(i,spout_num,percentile,nskip,duration):
    tuple_latencies = list()
    split_latencies = list()
    count = 0
    skipcount = 0
    with open('metrics/'+'metrics'+i+'.log') as metric_file:
        for line in metric_file:
            if skipcount>nskip*spout_num and count<=duration*spout_num:
                s = line
		split_latencies.append(s[s.find("[")+1:s.find("]")].replace(" ","").split(','))
                tuple_latencies.extend(s[s.find("[")+1:s.find("]")].replace(" ","").split(','))
                count+=1
            else:
                skipcount+=1
    latencies = list()
    interval_latencies = [[]]
    count = 0
    index = 0
    for latency in split_latencies:
        #print len(latency)
        lat = list()
        for l in latency:
            if not l == '':
                lat.append(l)
        if count == spout_num:
            count = 0
            index = index + 1
            interval_latencies.append(list(lat))
        else:
            interval_latencies[index].extend(list(lat))
            count = count + 1 
    print len(interval_latencies)
    #print interval_latencies[0]
    for i in range(20,len(interval_latencies)-20,20):
        #digest = TDigest()
        print i
        array = map(int,reduce(iadd, (interval_latencies[n] for n in range(1,i+1))))
        print len(array)
        print numpy.percentile(array,percentile)
        #digest.batch_update(array)
        #print digest.percentile(percentile)
 
    #for latency in tuple_latencies:
    #	if not latency =='':
    #        latencies.append(latency)
    #print len(latencies)
    #if len(latencies)>0:
    #    print numpy.percentile(map(int,latencies),percentile)
    #else:
    #    return float("inf"),float("inf"),float("inf")

tail_latency(sys.argv[1],15,99,10,180)
