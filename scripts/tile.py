import yaml
import sys
import random
import os
import json
import csv
from os.path import exists
from itertools import islice
import numpy
import shelve
import glob
import heapq

def nth_largest(n,iter):
    return heapq.nlargest(n,iter)[-1]

def tail_latency(i,spout_num,percentile,nskip):
   tuple_latencies = list()
   count = 0
   skipcount = 0
   with open('metrics/'+'metrics'+i+'.log') as metric_file:
       for line in metric_file:
           #if not line.find("latencies") == -1 and skipcount>nskip*spout_num:
           if skipcount>nskip*spout_num:
               s = line
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
   for latency in tuple_latencies:
   	if not latency =='':
           latencies.append(latency)
   #latencies = numpy.random.randint(0,200,1000000)
   n = len(latencies)
   k = 10000

   pcntl = 0.05
   pcnth = 0.05

   ksamp = numpy.sort(numpy.random.choice(latencies,k,replace=False))
   t = n-((n*percentile)/100)

   indexl = max(((n-t)/n)*k-pcntl*k,1)
   indexh = min(((n-t)/n)*k+pcnth*k,k)

   cutofl = ksamp[indexl]
   cutofh  = ksamp[indexh]
   
   #time1 = time.time()
   print numpy.percentile(map(int,latencies),percentile)
   #print time.time()-time1
   #time1 = time.time()
   print numpy.percentile(map(int,ksamp),percentile)
   #print time.time()-time1
tail_latency("0",12,99,10)

