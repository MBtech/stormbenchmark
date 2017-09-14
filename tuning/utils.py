import subprocess
import pandas
import sys
import random
import math
import os
import yaml
from pyDOE import *
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

def selective_write(select, fname ,basefile):
    stream = open(os.path.expanduser('~')+"/.storm/" + basefile, "r")
    docs = yaml.load(stream)

    for k,v in docs.items():
        #x,y in refdoc.items()
        if k in select:
            docs[k] = select[k]
        if isinstance(v, basestring):
            docs[k] = str(v)
    #print "\n"

    with open(fname, 'w') as yaml_file:
        yaml_file.write( yaml.dump(docs, default_flow_style=False))

def select_write(values, fname):
    stream = open(os.path.expanduser('~')+"/.storm/rollingcount.yaml", "r")
    docs = yaml.load(stream)

    for k,v in docs.items():
        #x,y in refdoc.items()
        for key in values:
            if key in k and 'topology' not in k and 'benchmark' not in k:
                print key
                docs[k] = values[key]
        if isinstance(v, basestring):
            docs[k] = str(v)
    #print "\n"
    print docs
    with open(fname, 'w') as yaml_file:
        yaml_file.write( yaml.dump(docs, default_flow_style=False))

def roundMultiple(number, multiple):
    num = number + (multiple-1)
    return int(num - (num % multiple))

# Write the configuration to the file
# Can write multiple entries based on the start end end entries
def write(design_space, start,end ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(start,end):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)

def utility_function(metric,cw,i):
    metrics = metric.split(',')
    limits = dict()
    improve_metric = ''
    for m in metrics:
        if '=' in m:
            limits[m.split('=')[0]] = int(m.split('=')[1])
        else:
            improve_metric = m
    for m in limits.keys():
        if cw[m][list(cw['no.']).index(i)]>=limits[m]:
           return cw[improve_metric][list(cw['no.']).index(i)]
    return sys.maxint

# Get results for a particular configuration. returns max int if the configuration failed to run
def get_results(start, end, design_space, basefile, metric):
    write(design_space, start,end ,basefile)
    for i in range(start, end):
        bashCommand = "./onescript.sh " + str(i)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]

    cw = pandas.read_csv("numbers.csv")
    metric_values = list()
    ret_array = list()
    for i in range(start, end):
        if i in list(cw['no.']):
            ret_array.append(i)
            metric_values.append(utility_function(metric,cw,i))
        else:
           ret_array.append(i)
           metric_values.append(sys.maxint)

    print metric_values
    print ret_array
    return metric_values,ret_array

# Generate a random point in the parameter search space
def generate_random(result,start,end,step,typ,relations,p,conf):
    i = 0
    for c in conf:
        if c in typ.keys():
            if typ[c] == "boolean":
                result[c] = random.choice([True, False])
            if typ[c] == "exp":
                steps = (end[c] - start[c])/step[c]
                result[c] = pow(2,(start[c] + (step[c]*random.randint(0,steps))))
        else:
            if "component.spout" in c or "topology.acker" in c:
                result[c] = result["topology.workers"]
            else:
                steps = (end[c] - start[c])/step[c]
                result[c] = roundMultiple(start[c] + (step[c]*random.randint(0,steps)),step[c])
        if c in relations:
                for e in relations[c]:
                    result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
                    if result[c]*1.1>result[e]:
                        result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
        i = i +1
    return result
def generate_LHS(result,start,end,step,typ,relations,p,conf,size):
    lhm = lhs(len(conf),samples=size)
    lhs_space = list(dict())
    for i in range(0,len(lhm)):
        sample = lhm[i]
        j =0
        print sample
        #print typ
        for c in conf:
            if c in typ.keys():
                if typ[c] == "boolean":
                # FIXME: This shouldn't be a random choice
                    result[c] =  random.choice([True,False])
                else:
                    result[c] = pow(2,int(start[c] + (end[c] - start[c])*sample[j]))
            else:
                result[c] = roundMultiple(int(start[c] + (end[c] - start[c])*sample[j]), step[c])
            if c in relations:
                for e in relations[c]:
                    result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
                    if result[c]*1.1>result[e]:
                        result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
            j +=1
        lhs_space.append(dict(result))
    return lhs_space
