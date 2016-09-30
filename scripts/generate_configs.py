import yaml
import random
import os
import math
from randomize import selective_write
import itertools
from pyDOE import *
import sys
from collections import OrderedDict
# Returns an ordered dictionary based on the order in which the parameters were found in the file
#http://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts
def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
     class OrderedLoader(Loader):
         pass
     def construct_mapping(loader, node):
         loader.flatten_mapping(node)
         return object_pairs_hook(loader.construct_pairs(node))
     OrderedLoader.add_constructor(
         yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
         construct_mapping)
     return yaml.load(stream, OrderedLoader)

def write(design_space ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(len(design_space)):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)

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

def generate_random(result,start,end,step,typ,p,conf,s,relations):
    i = 0
    denom = 0 
    steps = 0
    for c in conf:
        if s==c:
            steps = p[i]-1
	    denom = p[i]-1
        else:
            steps = p[i]
            denom = p[i]-1
	if c in typ.keys():
            if typ[c] == "boolean":
	        result[c] = random.choice([True, False])
            if typ[c] == "exp":
                result[c] = pow(2,(start[c] + (((end[c]-start[c])/denom) * random.randint(0,steps-1))))
	else:
	    result[c] = start[c] + (((end[c]-start[c])/denom) * random.randint(0,steps-1))
        if c in relations:
            for e in relations[c]:
                result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
                if result[c]*1.1>result[e]:
                    result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
        i = i +1
    return result

def step_up(result,start,end,step,typ,p,c,relations):
    if c in typ.keys():
        if typ[c] == "boolean":
            if result[c]==True: result[c]=False;
            else: result[c]=True;
        if typ[c] =="exp":
            result[c] = pow(2,math.log(result[c],2) + ((end[c]-start[c])/(p-1)))
    else:
        result[c] = result[c] + ((end[c]-start[c])/(p-1))
        if result[c] > end[c]:
            result[c] = end[c]
    
    if c in relations:
        for e in relations[c]:
            result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
            if result[c]*1.1>result[e]:
                result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
    return result
    
def ee_design(conf,sample,start,end,step,typ, basefile,relations):
    #p = [4,4,4,4,4,4,3,3]
    #p = [4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    p = [3,4,4,4,4,4,3,2,3,2,4,3]
    design_space=list()
    result = dict(sample)
    index = 0
    r = 8
    for c in conf:
        for i in range(0,r):
            #print c,index
            result = generate_random(result,start,end,step,typ,p,conf,c,relations)
            design_space.append(dict(result))
            print result
            step_up(result,start,end,step,typ,p[index],c,relations)
            design_space.append(dict(result))
            print result
        index = index + 1
    write(design_space, basefile)    

def main():
    # python generate_configs.py conf.yaml rollingcount.yaml relations.yaml
    conf_file = sys.argv[1]
    basefile = sys.argv[2]
    ref = open(conf_file, "r")
    sample = yaml.load(ref)
    result = dict(sample)
    start = dict()
    end = dict()
    step = dict()
    typ = dict()
    ref = open(conf_file, "r")
    conf = ordered_load(ref, yaml.SafeLoader).keys()
    #conf = sample.keys()
    #print conf
#    conf = ["component.split_bolt_num","component.rolling_count_bolt_num","component.rank_bolt_num","component.spout_num","topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
    for k in sample:
        vrange = sample[k]
        if len(vrange.split(","))==2:
            start[k] = int(vrange.split(",")[0])
            end[k] = int(vrange.split(",")[1])
        if len(vrange.split(","))==3:
	    start[k] = int(vrange.split(",")[0])
	    end[k] = int(vrange.split(",")[1])
            step[k] = int(vrange.split(",")[2])
        if len(vrange.split(","))==4:
	    typ[k] = vrange.split(",")[3]
            if vrange.split(",")[2] != "null":
                step[k] = int(vrange.split(",")[2])	    
                start[k] = int(vrange.split(",")[0])
                end[k] = int(vrange.split(",")[1])
    print start
    print end 
    print step
    print typ
    relation_file = sys.argv[3]
    rel = open(relation_file, "r")
    rel_dict = dict(yaml.load(rel))
    relations = dict()
    for r in rel_dict:
        split = rel_dict[r].split(",")
        relations[r] = list(split[:len(split)-1])
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
    ee_design(conf,sample,start,end,step,typ,basefile,relations)
 
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


