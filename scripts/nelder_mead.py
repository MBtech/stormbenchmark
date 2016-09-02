import random
import os
import math
import numpy

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

from utils import get_results
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

# Write the configuration to the file
# Can write multiple entries based on the start end end entries
def write(design_space, start,end ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(start,end):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)

# Generate a random point in the parameter search space
def generate_random(result,start,end,step,typ,p,conf):
    i = 0
    denom = 0
    steps = 0
    for c in conf:
        steps = p[i]
        denom = p[i]-1
        if c in typ.keys():
            if typ[c] == "boolean":
                result[c] = random.choice([True, False])
            if typ[c] == "exp":
                result[c] = pow(2,(start[c] + (((end[c]-start[c])/denom) * random.randint(0,steps-1))))
        else:
            result[c] = start[c] + (((end[c]-start[c])/denom) * random.randint(0,steps-1))
        i = i +1
    return result

def put_limits(value,start,end):
    if value<start:
        return start
    else:
        return end
    
# Create a centroid
def centroid(x_list,result,start,end,step,typ,conf):
    i = 0
    denom = 0
    steps = 0
    center = dict()
    for c in conf:
        if c not in typ.keys():
 #           print x_list
            center[c] = 0
            for i in range(0,len(x_list)-1):
                center[c] += x_list[i][c]
#            print center[c]
            center[c] = roundMultiple(center[c]/(len(x_list)-1),3)
        else:
            if typ[c] == "boolean":
                bool_list = list()
                for i in range(0,len(x_list)-1):
                    bool_list.append(x_list[i][c])
                if numpy.median(bool_list)==0.0:
                    center[c] = False
                else:
                    center[c] = True
            if typ[c] == "exp":
                cat_list = list()
                for i in range(0,len(x_list)-1):
                    cat_list.append(x_list[i][c])
                center[c] = numpy.median(cat_list)
    return center

def reflection(x0, xn1, alpha, conf, typ,start,end,step):
    xr = dict()
    for c in conf:
        if c not in typ.keys():
            xr[c] = put_limits(roundMultiple(x0[c] + alpha * (x0[c] - xn1[c]), step[c]),start[c],end[c])
        else:
            if typ[c] == "boolean":
                xr[c] = x0[c] + alpha*(x0[c] - xn1[c])
            if typ[c] == "exp":
                xr[c] = put_limits(pow(2, int(math.log(x0[c]) + alpha*(math.log(x0[c]) - math.log(xn1[c])))), pow(2,start[c]),pow(2,end[c]))
    return xr

def expansion(x0, xr, gamma, conf, typ,start,end,step):
    xe = dict()
    for c in conf:
        if c not in typ.keys():
            xe[c] = put_limits(roundMultiple(x0[c] + gamma * (xr[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                xe[c] = x0[c] + gamma*(xr[c] - x0[c])
            if typ[c] == "exp":
                xe[c] = put_limits(pow(2, int(math.log(x0[c]) + gamma*(math.log(xr[c]) - math.log(x0[c])))),pow(2,start[c]),pow(2,end[c]))
    return xe

def contraction(x0, xn1, phi, conf, typ,start,end,step):
    xc = dict()
    for c in conf:
        if c not in typ.keys():
            xc[c] = put_limits(roundMultiple(x0[c] + phi* (xn1[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                xc[c] = x0[c] + phi*(xn1[c] - x0[c])
            if typ[c] == "exp":
                xc[c] = put_limits(pow(2, int(math.log(x0[c]) + phi*(math.log(xn1[c]) - math.log(x0[c])))), pow(2,start[c]),pow(2,end[c]))
    return xc

def shrink(x0, xi, sigma, conf, typ,start,end,step):
    new_xi = dict()
    for c in conf:
        if c not in typ.keys():
            new_xi[c] = put_limits(roundMultiple(x0[c] + sigma* (xi[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                new_xi[c] = x0[c] + sigma* (xi[c] - x0[c])
            if typ[c] == "exp":
                new_xi[c] = put_limits(pow(2, int(math.log(x0[c]) + sigma*(math.log(xi[c]) - math.log(x0[c])))), pow(2,start[c]),pow(2,end[c]))
    return new_xi

def neldermead(conf,sample,start,end,step,typ, basefile, metric):
    # Initializations
    phi = 0.5
    sigma = 0.5
    alpha = 1
    gamma = 2
    n = len(conf)
    total_runs = 50
#    n = 4
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    metric_values = list()
    # Generate the first n samples
    while len(design_space)<n+1:
        result = generate_random(result,start,end,step,typ,p,conf)
        design_space.append(dict(result))
        values,elements = get_results(len(design_space)-1, len(design_space),design_space,basefile,metric)
        if values[0]==sys.maxint:
            del design_space[-1]
        else:
            metric_values.extend(values)
#    print metric_values
    while len(design_space)<=total_runs:
        fx = sorted(metric_values)
        x_sorted = [x for (y,x) in sorted(zip(metric_values,design_space), key=lambda pair: pair[0])]
        print fx[0]
        print x_sorted[0]
        # fx1 = min(metric_values)
        # x1 = design_space[metric_values.index(fx1)]
        
        x0 = centroid(x_sorted[:len(x_sorted)-1],result,start,end,step,typ,conf)

        # Reflection Step
        xr = reflection(x0, x_sorted[len(x_sorted)-1], alpha, conf,typ, start,end,step)
        design_space.append(xr)
        fx_r,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile,metric)
        if fx_r < fx[len(fx)-2] and fx_r >= fx[0]:
            x_sorted[len(x_sorted)-1] = dict(xr)
            fx[len(fx)-1] = fx_r
            continue

        # Expansion step
        elif fx_r < fx[0]:
            xe = expansion(x0, xr, gamma, conf, typ,start,end,step)
            design_space.append(xe)
            fx_e,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile, metric)
            if fx_e < fx_r:
                x_sorted[len(x_sorted)-1] = dict(xe)
                fx[len(fx)-1] = fx_e
                continue
            else:
                x_sorted[len(x_sorted)-1] = dict(xr)
                fx[len(fx)-1] = fx_r
                continue

        # Contraction Step
        else:
            xc = contraction(x0, x_sorted[len(x_sorted)-1], phi, conf, typ, start,end,step)
            design_space.append(xc)
            fx_c,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile, metric)
            if fx_c < fx[len(fx)-1]:
                x_sorted[len(x_sorted)-1] = dict(xc)
                fx[len(fx)-1] = fx_c
                continue
            # Shrink Step
            else:
                for i in range(1,n+1):
                    x_sorted[i] = shrink(x_sorted[0], x_sorted[i], sigma, conf,typ, start,end,step)
                    continue

def main():
    # python generate_configs.py conf.yaml lat_90
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
    neldermead(conf,sample,start,end,step,typ,basefile,metric)

if __name__ == '__main__':
    main()

