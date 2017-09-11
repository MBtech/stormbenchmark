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
import subprocess
import re
from utils import *

def tail_latency(i,spout_num,percentile,nskip):
    '''
    tuple_latencies = list()
    count = 0
    skipcount = 0
    with open('metrics/'+'metrics'+i+'.log') as metric_file:
        for line in metric_file:
            #if not line.find("latencies") == -1 and skipcount>nskip*spout_num:
            if not skipcount>nskip*spout_num:
                s = line
                tuple_latencies.extend(s[s.find("[")+1:s.find("]")].replace(" ","").split(','))
                count+=1
                #print line
            else:
                skipcount+=1
                #print line
    print count
    print skipcount
    print len(tuple_latencies)
    latencies = list()
    for latency in tuple_latencies:
    	if not latency =='':
            latencies.append(latency)
    print len(latencies)
    if len(latencies)>0:
        median= numpy.percentile(map(int,latencies),50)
        #print numpy.percentile(map(int,latencies),percentile)
        return median,numpy.percentile(map(int,latencies),90),numpy.percentile(map(int,latencies),percentile)
    else:
        return float("inf"),float("inf"),float("inf")
    #histogram.histogram(tuple_latencies, 'latency-histogram')
    #trace1 = go.Box(
    #y=tuple_latencies,
    #)
    #data = [trace1]
    #fig = go.Figure(data=data)
    #plot_url = plotly.offline.plot(fig, filename='box-plot-latency')
    '''
    lat = list()
    for i in range(5, 10):
        args = ['java', '-cp', '/home/ubuntu/bilal/TDigestService/target/TDigestService-1.0-SNAPSHOT-jar-with-dependencies.jar', 'com.tdigestclient.Main', '127.0.0.1' ,'11111', str((1.0*i)/10)]
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        m = re.search('[0-9].*',p.stdout.readline())
       	if m!=None:
	    lat.append(m.group(0)) 
    args = ['java', '-cp', '/home/ubuntu/bilal/TDigestService/target/TDigestService-1.0-SNAPSHOT-jar-with-dependencies.jar', 'com.tdigestclient.Main', '127.0.0.1' ,'11111', str(0.99)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    m = re.search('[0-9].*',p.stdout.readline())
    if m!=None:
        lat.append(m.group(0))
    return lat

def compare_capacity(cap):
    return (sum([cap[x] for x in cap])**2)/(len(cap)*sum([cap[x]**2 for x in cap]))

def getnext(components,nprocs,best,current):
    threads = dict()
    index = 2
    for c in components:
        threads[c] =max(int(nprocs*round(int(round((int(best[index])+int(current[index]))/2))/nprocs)),nprocs)
        index+=1
    return threads

def find_entry(threads):
    entries = list()
    with open('numbers.csv','r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        entries = list(reader)
    for entry in entries:
        print entry[2:4]
	print threads
        if len(set(entry[2:4]).intersection(threads))>0:
            return True
    return False

def read_last_entries(index):
    entries = list()
    with open('numbers.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')
        entries = list(islice(reader,index+2))
        print entries
    return [entries[-2],entries[-1]]

def read_entries():
    entries = list()
    with open('numbers.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')
        entries = list(reader)
        print entries
    return entries

def read_best(filename,key):
    d = shelve.open(filename)
    best = d[key]
    d.close()
    return best

def write_best(filename,key,best):
    d = shelve.open(filename)
    d[key] = best
    d.close()
# def binary_dec(components,best,current,nprocs,baseline):

#     if float(baseline)<float(current[8]):
#         return getnext(components,nprocs,best,current)
#     if float(baseline)>float(current[8]) and (int(best[3])+int(best[2]))>(int(current[3])+int(current[2])):
#         write_best('best.log','best',current)
#         return getnext(components,nprocs,best,current)
#     else:
#         return getnext(components,nprocs,best,current)
def binary_dec(components,best,current,nprocs,index,metric_index):
    threads = dict()
    if float(best[metric_index])>float(current[metric_index]):
        write_best('best.log','best',current)
        #data = read_last_entries(int(index))
        threads = getnext(components,nprocs,best,current)

    else:
        threads = getnext(components,nprocs,best,current)
    return threads

def multiplicative_dec(cap,components,bolt_threads,nprocs,total_capacity,platency):
    threads = dict()
    print "The fairness result is " + str(compare_capacity(cap))
    #The suitable value for the Jain's Fairness. A value closer to 1 means more fair
    alpha = 0.99
    for c in components:
        if cap[c]<0.5:
            threads[c]=max(int(nprocs*round(int(round(bolt_threads[c]/2))/nprocs)),nprocs)
    else:
            threads[c] = bolt_threads[c]
    if compare_capacity(cap)<0.99:
        for c in components:
            percent = cap[c]/total_capacity
            print "Percentage of total capcity for component " + c +" is "+ str(percent)
            #threads[c] = max(int(round(percent*sum(bolt_threads.values()))),nprocs)
    else:
        for c in components:
            print "Processing latency for component " + c +" is "+ str(platency[c])
            #percent = platency[c]/max(platency.values())
            #threads[c] = max(int(round(percent*bolt_threads[c])),nprocs)
            # percent = platency[c]/total_latency
            # threads[c] = int(round(percent*sum(bolt_threads.values())))
    return threads



def component_info(direc,index,components,nthreads,nprocs,lat,tolerance,duration):
    cap = dict()
    platency = dict()
    elatency = dict()
    spout_threads = 0
    bolt_threads = dict()
    throughput = 0
    latency = 0.0
    # Get the throughput number
    with open(direc+'topology'+index+'.json') as data_file:
        data = json.load(data_file)
        for i in data['spouts']:
            throughput = int(i['acked']/(duration-10))
            latency = float(i['completeLatency'])

    #Extract the average capacity and processing latency info
    #for c in components:
    #    print c+"\n"
    #    with open(direc+c+'_'+index+'.json') as data_file:
    #        data = json.load(data_file)
    #        capacity = 0
    #        process_latency = 0
    #        execute_latency = 0
    #        for i in data['executorStats']:
    #            #print "Capacity: "+i['capacity']
    #            capacity += float(i['capacity'])
    #            #print "Processing Latency: " +i['processLatency']
    #            process_latency += float(i['processLatency'])
    #            #print "Execute Latency: " + i['executeLatency']
    #            execute_latency += float(i['executeLatency'])

    #       cap[c] = capacity/len(data['executorStats']);
    #        platency[c] = process_latency/len(data['executorStats']);
    #        elatency[c] = execute_latency/len(data['executorStats']);
    #total_latency = sum(platency.values())
    #total_capacity = sum(cap.values())

    threads = dict()
    with open(direc+'topology'+index+'.json') as data_file:
        data = json.load(data_file)
        for c in data['spouts']:
            spout_threads+=int(c['executors'])

        for c in data['bolts']:
            bolt_threads[c['boltId']] = int(c['executors'])
     
    fieldnames = list()
    data = list()
    fieldnames.append("no.")
    data.append(index)
    fieldnames.append("spout")
    data.append(spout_threads)
    for c in bolt_threads.keys():
        fieldnames.append(c)
        data.append(bolt_threads[c])
    #for c in bolt_threads.keys():
    #    fieldnames.append("cap_" + c)
    #    data.append(format(cap[c],'.2f'))
    fieldnames.append("throughput")
    data.append(throughput)
    '''fieldnames.append("latency")
    data.append(median)
    #data.append(latency)
    fieldnames.append("90th perct latency")
    data.append(tail_90)
    fieldnames.append("99th perct latency")
    data.append(tail_99)'''
    for i in range(50,100,10):
        fieldnames.append("lat_" + str(i))
        data.append(lat[(i-50)/10])
    fieldnames.append("lat_99")
    data.append(lat[len(lat)-1])   
    flag = False
    flag = exists('numbers.csv')
    with open('numbers.csv', 'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames, delimiter = ',')
        if not flag:
            writer.writeheader()
        writer = csv.writer(csvfile, delimiter = ',')
        writer.writerow(data)

    #Spout thread entry but needs to be changed if there are multiple spouts
    threads['spout'] = spout_threads
    return threads,data

def optimize(components,threads,data,nprocs,index,tolerance,indicies):
    best = list()
    metric_index = indicies["metric"]
    if int(index)==0:
        #threads = multiplicative_dec(cap,components,bolt_threads,nprocs,total_capacity,platency)
        for c in components:
            threads[c] = 2*nprocs
        fname = "config_files/"+"test"+str(int(index)+1)+".yaml"
        select_write(threads,fname)
        best = write_best('best.log','best',data)
        baseline = write_best('best.log','baseline',data[metric_index]*tolerance)
    else:
        data = read_last_entries(int(index))
        best = read_best('best.log','best')
        baseline = read_best('best.log','baseline')
        #if data[1][metric_index]<best[metric_index] and int(index)==1:
        #    print "No more files"
        #    return
        #res_indicies = indicies["resources"]
        flag = True

        threads = binary_dec(components,best,data[1],nprocs,index,metric_index)
        if not find_entry(map(str,threads.values())):
            fname = "config_files/"+"test"+str(int(index)+1)+".yaml"
            select_write(threads,fname)
            flag = False
        print flag
        #for i in res_indicies:
        #    flag = flag and data[0][i]==data[1][i]
        if flag:
            data = read_last_entries(0)
            if best[metric_index]<data[1][metric_index]:
                print "Best configuration is: " + str(best)
                write_best('best.log','baseline',float(best[metric_index])*tolerance)
                baseline = read_best('best.log','baseline')
                data = read_entries()
		suitable = list()
                for entry in data[1:]:
                    if not suitable:
                        if float(baseline)>float(entry[metric_index]) and (int(best[3])+int(best[2]))>(int(entry[3])+int(entry[2])):
                            suitable = list(entry)
                            print "Best configuration under constraints is: " + str(entry)
                    elif float(baseline)>float(entry[metric_index]) and (int(suitable[3])+int(suitable[2]))>(int(entry[3])+int(entry[2])):
                        suitable = list(entry)
                        print "Best configuration under constraints is: " + str(entry)
                if suitable:
                    pcandidate = list(suitable)
                else:
                    pcandidate = list(best)
                print pcandidate
                pcandidate[3]=str(int(pcandidate[3])-nprocs)
                pcandidate[2]=str(int(pcandidate[2])-nprocs)
                if not find_entry([pcandidate[2],pcandidate[3]]):
		    fname = "config_files/"+"test"+str(int(index)+1)+".yaml"
                    select_write([pcandidate[2],pcandidate[3]],fname)
                else:
                    print "No more files"

    print threads
    #for c in components:
    #    print c
    #    print threads[c]

def getduration():
    newest_file = max(glob.iglob('reports/*.csv'), key=os.path.getctime)
    with open(newest_file) as f:
        for i, l in enumerate(f):
            pass
    return i

def main():
    direc = sys.argv[1]
    index = sys.argv[2]
    nthreads = int(sys.argv[3])
    nprocs = int(sys.argv[4])
    spout_num = int(sys.argv[5])
    percentile = int(sys.argv[6])
    skip_intervals = int(sys.argv[7])
    tolerance = float(sys.argv[8])
    if spout_num==0:
        loaded_data = yaml.load(open("config_files/test"+index+".yaml",'r'))
        spout_num = int(loaded_data["component.spout_num"])
    lat = tail_latency(index,spout_num,percentile,skip_intervals)
    print lat
    #print tail_99
    components = ['rolling_count','split']
    #Get the duration of the experiment to calculate the average throughput
    duration = int(getduration())*10
    print "duration: " + str(duration)
    threads,data=component_info(direc, index,components, nthreads, nprocs,lat,tolerance,duration)
    indicies = dict()
    indicies["resources"]=[2,3]
    indicies["metric"]=9
    #optimize(components,threads,data,nprocs,index,tolerance,indicies)

if __name__ == '__main__':
    main()
