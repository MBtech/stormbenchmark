import yaml
import random
import os
import math
from randomize import selective_write
import itertools
from pyDOE import *

def write(design_space):
    folder = "config_files1"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(len(design_space)):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname)

def factorial_design(conf, sample,start,end,step):
    design_space = list()
    table = ff2n(len(conf))
    for index in range(0,128):
        result = dict(sample)
        j = 0
        for i in table[index]:
            if i == -1.0:
                result[conf[j]] = start[conf[j]]
            else:
                result[conf[j]] = end[conf[j]]
            j = j+1
        print result
        design_space.append(dict(result))


    write(design_space)

def oat_design(conf, sample, start,end,step):
    design_space=list()
    result = dict(sample)   
    for c in conf: 
        result[c] = start[c]
    
    design_space.append(dict(result))
    for c in conf:
        for index in range(1,4):
            temp = dict(result)
	    inc = start[c] + index*step[c]
	    if inc<=end[c]:
                temp[c] = inc	
            else:
                temp[c] = end[c]
            design_space.append(dict(temp))
    write(design_space)

def generate_random(result,start,end,step,p,conf):
    i = 0
    for c in conf:
	result[c] = start[c] + (((end[c]-start[c])/(p[i]-1)) * random.randint(0,p[i]-2))
        i = i +1
    return result

def step_up(result,start,end,step,p,c):
    result[c] = result[c] + ((end[c]-start[c])/(p-1))
    if result[c] <= end[c]:
	return result
    else:
        result[c] = end[c]
        return result
    
def ee_design(conf,sample,start,end,step):
    p = [4,4,4,4,4,3,3]
    design_space=list()
    result = dict(sample)
    index = 0
    r = 4
    for c in conf:
        for i in range(0,r):
            #print c,index
            result = generate_random(result,start,end,step,p,conf)
            design_space.append(dict(result))
            print result
            step_up(result,start,end,step,p[index],c)
            design_space.append(dict(result))
            print result
        index = index + 1
    write(design_space)    

def main():
    ref = open("conf1.yaml", "r")
    sample = yaml.load(ref)
    result = dict(sample)
    start = dict()
    end = dict()
    step = dict()
    conf = ["component.rolling_count_bolt_num","component.split_bolt_num","component.spout_num","topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
    for k in sample:
        vrange = sample[k]
        if len(vrange.split(","))==2:
            start[k] = int(vrange.split(",")[0])
            end[k] = int(vrange.split(",")[1])
        if len(vrange.split(","))==3:
	    start[k] = int(vrange.split(",")[0])
	    end[k] = int(vrange.split(",")[1])
            step[k] = int(vrange.split(",")[2])
    print start
    print end 
    print step
    '''print start
    print end
    print step
    flag = True
    index = 0
    while index<10:
        result = dict(sample)
        for k in sample:
            result[k] = start[k]+(index*step[k])
        index = index +1
        design_space.append(dict(result))'''
    #table = list(itertools.product([False, True], repeat=128))
    
    #factorial_design(conf,sample)
    #oat_design(conf,sample,start,end,step)
    ee_design(conf,sample,start,end,step)
 
def factorial_design(conf, sample):
    design_space = list()
    table = ff2n(len(conf))
    index = 0
    for index in range(0,128):
        result = dict(sample)
        j = 0
	for i in table[index]:
            if i == -1.0:
                result[conf[j]] = start[conf[j]]
	    else: 
                result[conf[j]] = end[conf[j]]
            j = j+1
        print result
        design_space.append(dict(result))
            
            
    write(design_space)

if __name__ == '__main__':
    main()


