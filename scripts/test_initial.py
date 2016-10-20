import yaml
import random
import os
import math
import numpy
import sys
from collections import OrderedDict
from randomize import selective_write
import itertools
import subprocess
import pandas
from pyDOE import *
import warnings
import generate_initial
import pickle

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

# Get the number rounded to multiple of a particular number
def roundMultiple(number, multiple):
    num = number + (multiple-1)
    return int(num - (num % multiple))

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
# Write the configuration to the file
# Can write multiple entries based on the start end end entries
def write(design_space, start,end ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(start,end):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i-120],fname, basefile)

# Generate a random points using LHS
def generate_LHS(result,start,end,step,typ,relations,p,conf,size):
    lhm = lhs(len(conf),samples=size, criterion="center")
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

# Get results for the specific configurations
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

# Get the metric readings for all experiments
def get_metric(metric):
    cw = pandas.read_csv("numbers.csv")
    metrics = list()
    for i in range(0,len(cw)):
        metrics.append(utility_function(metric,cw,i))
    return metrics

def hill_climbing(conf,sample,start,end,step,typ, relations,basefile, metric):

    # Initializations
    p =[]
    total_runs = 50
    m = 30
    n = int(0.1*total_runs) # Number of local samples
    l = int(0.1*total_runs) # number of samples in the restart phase
    t = 0.4 # Threshold for neighborhood size
    alpha = 0.75 # The shrinking factor
    alpha_passed =alpha
    program_state = list()
    filename="programState"
    state = ""
    design_space = list()
    metric_values = list()
    numbers = list()
    result = dict(sample)
    np = n
    neighborhood_size = alpha_passed
    f_thresh = list()
    # Generate the first n samples using LHS
    #design_space = generate_initial.generate_initial(result,start,end,step,typ,relations,[],conf,m)
    design_space = generate_LHS(result,start,end,step,typ,relations,[],conf,m)
    # Get results and get the best configuration
    metric_values,numbers = get_results(120,120+m,design_space, basefile,metric)
    fx0 = min(metric_values)
    x0 = design_space[numbers[metric_values.index(fx0)]]
#    fx0,x0,design_space,metric_values,numbers = confirm(x0,design_space,basefile,metric,m,m+1,metric_values,numbers)
    n_start = dict(start)
    n_end = dict(end)
    local_search = True
    restart = True
    x_center = dict(x0)
    fx_center = fx0
    state = "localsearch"


def main():
    warnings.simplefilter('ignore', numpy.RankWarning)
    # python rrs.py conf.yaml rollingtopwords.yaml lat_90 relations.yaml
    conf_file = sys.argv[1]
    basefile = sys.argv[2]
    metric = sys.argv[3]
    ref = open(conf_file, "r")
    sample = yaml.load(ref)
    result = dict(sample)
    start = dict(); end = dict(); step = dict(); typ = dict()
    ref = open(conf_file, "r")
    conf = ordered_load(ref, yaml.SafeLoader).keys()
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

    relation_file = sys.argv[4]
    rel = open(relation_file, "r")
    rel_dict = dict(yaml.load(rel))
    relations = dict()
    for r in rel_dict:
        split = rel_dict[r].split(",")
        relations[r] = list(split[:len(split)-1])
    
    print start
    #print relations
    #for i in range(0,9):
    hill_climbing(conf,sample,start,end,step,typ,relations,basefile,metric)

if __name__ == '__main__':
    main()

