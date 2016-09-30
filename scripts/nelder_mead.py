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
from utils import generate_random
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

def put_limits(value,start,end):
    if value<start:
        return start
    else:
        return end
    
# Create a centroid
def centroid(x_list,result,start,end,step,typ,conf,relations):
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
        if c in relations:
                for e in relations[c]:
                    center[e] = pow(2,int(math.ceil(math.log(center[c],2))))
                    if center[c]*1.1>center[e]:
                        center[e] = pow(2,int(math.ceil(math.log(center[c],2)))+1)
    return center

def reflection(x0, xn1, alpha, conf, typ,start,end,step,relations):
    xr = dict()
    for c in conf:
        if c not in typ.keys():
            xr[c] = put_limits(roundMultiple(x0[c] + alpha * (x0[c] - xn1[c]), step[c]),start[c],end[c])
        else:
            if typ[c] == "boolean":
                xr[c] = bool(round(x0[c] + alpha*(x0[c] - xn1[c])))
            if typ[c] == "exp":
                xr[c] = put_limits(pow(2, int(math.log(x0[c]) + alpha*(math.log(x0[c]) - math.log(xn1[c])))), pow(2,start[c]),pow(2,end[c]))
        if c in relations:
                for e in relations[c]:
                    xr[e] = pow(2,int(math.ceil(math.log(xr[c],2))))
                    if xr[c]*1.1>xr[e]:
                        xr[e] = pow(2,int(math.ceil(math.log(xr[c],2)))+1)
    return xr

def expansion(x0, xr, gamma, conf, typ,start,end,step,relations):
    xe = dict()
    for c in conf:
        if c not in typ.keys():
            xe[c] = put_limits(roundMultiple(x0[c] + gamma * (xr[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                xe[c] = bool(round(x0[c] + gamma*(xr[c] - x0[c])))
            if typ[c] == "exp":
                xe[c] = put_limits(pow(2, int(math.log(x0[c]) + gamma*(math.log(xr[c]) - math.log(x0[c])))),pow(2,start[c]),pow(2,end[c]))
        if c in relations:
                for e in relations[c]:
                    xe[e] = pow(2,int(math.ceil(math.log(xe[c],2))))
                    if xe[c]*1.1>xe[e]:
                        xe[e] = pow(2,int(math.ceil(math.log(xe[c],2)))+1)
    return xe

def contraction(x0, xn1, phi, conf, typ,start,end,step,relations):
    xc = dict()
    for c in conf:
        if c not in typ.keys():
            xc[c] = put_limits(roundMultiple(x0[c] + phi* (xn1[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                xc[c] = bool(round(x0[c] + phi*(xn1[c] - x0[c])))
            if typ[c] == "exp":
                xc[c] = put_limits(pow(2, int(math.log(x0[c]) + phi*(math.log(xn1[c]) - math.log(x0[c])))), pow(2,start[c]),pow(2,end[c]))
        if c in relations:
                for e in relations[c]:
                    xc[e] = pow(2,int(math.ceil(math.log(xc[c],2))))
                    if xc[c]*1.1>xc[e]:
                        xc[e] = pow(2,int(math.ceil(math.log(xc[c],2)))+1)
    return xc

def shrink(x0, xi, sigma, conf, typ,start,end,step,relations):
    new_xi = dict()
    for c in conf:
        if c not in typ.keys():
            new_xi[c] = put_limits(roundMultiple(x0[c] + sigma* (xi[c] - x0[c]), step[c]), start[c],end[c])
        else:
            if typ[c] == "boolean":
                new_xi[c] = bool(round(x0[c] + sigma* (xi[c] - x0[c])))
            if typ[c] == "exp":
                new_xi[c] = put_limits(pow(2, int(math.log(x0[c]) + sigma*(math.log(xi[c]) - math.log(x0[c])))), pow(2,start[c]),pow(2,end[c]))
        if c in relations:
                for e in relations[c]:
                    new_xi[e] = pow(2,int(math.ceil(math.log(new_xi[c],2))))
                    if new_xi[c]*1.1>new_xi[e]:
                        new_xi[e] = pow(2,int(math.ceil(math.log(new_xi[c],2)))+1)
    return new_xi

def neldermead(conf,sample,start,end,step,typ,relations, basefile, metric):
    # Initializations
    phi = 0.5
    sigma = 0.5
    alpha = 1
    gamma = 2
    n = len(conf)
    total_runs = 50
#    n = 4
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    metric_values = list()
    # Generate the first n samples
    print "Generating first " + str(n+1) + " points"
    while len(design_space)<=n+1:
        result = generate_random(result,start,end,step,typ,relations,p,conf)
        design_space.append(dict(result))
        values,elements = get_results(len(design_space)-1, len(design_space),design_space,basefile,metric)
        #if values[0]==sys.maxint:
        #    del design_space[-1]
        #else:
        metric_values.extend(values)
#    print metric_values
    vals = list(metric_values)
    configs = list(design_space)
    print "Sorting"
    fx = sorted(vals)
    x_sorted = [x for (y,x) in sorted(zip(vals,configs), key=lambda pair: pair[0])]

    while len(design_space)<=total_runs:
        # fx1 = min(metric_values)
        # x1 = design_space[metric_values.index(fx1)]
        print fx
        print x_sorted[0]        
        x0 = centroid(x_sorted[:len(x_sorted)-1],result,start,end,step,typ,conf,relations)
        print x0
        # Reflection Step
        print "Reflection"
        xr = reflection(x0, x_sorted[len(x_sorted)-1], alpha, conf,typ, start,end,step,relations)
        if xr in design_space:
            print "Repetition. Replacing it with another random point"
            xr = generate_random(result,start,end,step,typ,relations,p,conf)
            design_space.append(dict(xr))
        else:
            design_space.append(xr)
        fx_r,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile,metric)
        if fx_r < fx[len(fx)-2] and fx_r >= fx[0]:
            x_sorted[len(x_sorted)-1] = dict(xr)
            fx[len(fx)-1] = fx_r
            continue

        # Expansion step
        elif fx_r < fx[0]:
            print "Expansion"
            xe = expansion(x0, xr, gamma, conf, typ,start,end,step,relations)
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
            print "Contraction"
            xc = contraction(x0, x_sorted[len(x_sorted)-1], phi, conf, typ, start,end,step,relations)
            design_space.append(xc)
            fx_c,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile, metric)
            if fx_c < fx[len(fx)-1]:
                x_sorted[len(x_sorted)-1] = dict(xc)
                fx[len(fx)-1] = fx_c
                continue
            # Shrink Step
            else:
                print "Shrinking time"
                for i in range(1,n+1):
                    x_sorted[i] = shrink(x_sorted[0], x_sorted[i], sigma, conf,typ, start,end,step,relations)
                    design_space.append(x_sorted[i])
                vals,elements = get_results(len(design_space)-n, len(design_space),design_space,basefile,metric)
                fx = sorted(vals)
                x_sorted = [x for (y,x) in sorted(zip(vals,x_sorted), key=lambda pair: pair[0])]
                continue

def main():
    # python nelder_mead.py conf_rollingcount_hc.yaml rollingcount.yaml lat_90 relations.yaml
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

    neldermead(conf,sample,start,end,step,typ,relations,basefile,metric)

if __name__ == '__main__':
    main()

