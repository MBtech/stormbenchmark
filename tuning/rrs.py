import yaml
import random
import os
import math
import numpy
import sys
from collections import OrderedDict
import itertools
import subprocess
import pandas
from utils import *
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

def dict_product(dic):
    product = [x for x in apply(itertools.product, dic.values())]
    print product
    return [dict(zip(dic.keys(), p)) for p in product]

# Get the neighborhood of a point
def neighborhood(x0, u, l, r, n1, step, typ, conf):
    max_dist = dict()
    for c in conf:
        if c in typ.keys():
            if typ[c] == "boolean":
                continue
        max_dist[c] = pow(r,1.0/n1)*(u[c] -l[c])
    print max_dist
    neighbors = list(dict())
    neighbor = dict()
    ranges = dict()
    for c in conf:
        range_list = list()
        cont = True
        i = 1
        while cont:
            if c in typ.keys():
                if typ[c] == "boolean":
                    range_list.append(True)
                    range_list.append(False)
                    cont = False
                    #neighbor[c] = random.choice([True, False])
                if typ[c] == "exp":
                    z = math.log(x0[c],2) + i*step[c]
                    z1 = math.log(x0[c],2) - i*step[c]
                    print c,z,z1,max_dist[c],z-math.log(x0[c],2)
                    if abs(z - math.log(x0[c],2)) < max_dist[c]:
                        range_list.append(pow(2,min(z,u[c])))
                        range_list.append(pow(2,max(z1,l[c])))
                    else:
                        cont = False
            else:
                z = x0[c] + i*step[c]
                z1 = x0[c] - i*step[c]
                if abs(z - x0[c]) < max_dist[c]:
                    range_list.append(min(z,u[c]))
                    range_list.append(max(z1,l[c]))
                else:
                    cont = False
            i = i+1
        ranges[c] = list(set(range_list))
    prod =  [x for x in apply(itertools.product, ranges.values())]
    neighbors = [dict(zip(ranges.keys(), p)) for p in prod]
    return neighbors

#def rrs(result,end, start ,step,typ,conf):
def in_neighborhood(x0, xi, u, l, r, n1, step, typ, conf):
    max_dist = dict()
    for c in conf:
        if c in typ.keys():
            if typ[c] == "boolean":
                continue
        max_dist[c] = pow(r,1.0/n1)*(u[c] -l[c])
    print max_dist
    neighbors = list(dict())
    neighbor = dict()
    ranges = dict()
    cont = True
    for c in conf:
        range_list = list()

        i = 1
        if c in typ.keys():
            if typ[c] == "boolean":
                cont = True
                #neighbor[c] = random.choice([True, False])
            if typ[c] == "exp":
                if abs(math.log(xi[c],2) - math.log(x0[c],2)) < max_dist[c]:
                    cont = True
                else:
                    cont = False
        else:
            if abs(xi[c] - x0[c]) < max_dist[c]:
                cont = True
            else:
                cont = False
    return cont

# Get random point for the neighborhood of a point
def get_neighbor(result,start,end,step,typ,relations,phi,p,conf):
    xi = generate_random(result,start,end,step,typ,relations,p,conf)
    while not in_neighborhood(result, xi, end, start, phi, len(conf), step, typ, conf):
        xi = generate_random(result,start,end,step,typ,relations,p,conf)
    return xi

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

def rrs(conf,sample,start,end,step,typ, relations,basefile, metric):
    # Initializations
    v=0.5
    p=0.95
    q=0.9
    r=0.2
    c=0.5
    st=0.1
    #n should be around 23 and l should be around 6-7
    n=int(math.log(1-p)/math.log(1-r))
    l=int(math.log(1-q)/math.log(1-v))
    print n,l
    f_thresh = list()
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    # Generate the first n samples
    for i in range(0,n):
        result = generate_random(result,start,end,step,typ,relations,p,conf)
        design_space.append(dict(result))

    # Get results and get the best configuration
    metric_values,numbers = get_results(0,n,design_space, basefile,metric)
    fx0 = min(metric_values)
    print metric_values
    x0 = design_space[numbers[metric_values.index(fx0)]]
    f_gamma = fx0
    f_thresh.append(fx0)
    i =0
    flag = True
    xopt = dict(x0)
    fx_opt = fx0
    # List for next n samples
    new_x = list(dict())
    new_fx = list()
    while len(design_space)<=50:
        print "Best configuration" + str(xopt)
        print "Best result " + str(fx_opt)
        if flag:
            print "Exploitation Phase"
            j=0; fl = fx_opt; xl = dict(xopt); phi=r
            while phi>=st:
                x_bar = get_neighbor(xl,start,end,step,typ,relations,phi,p,conf)
                while x_bar in design_space:
                    x_bar = get_neighbor(xl,start,end,step,typ,relations,phi,p,conf)
                design_space.append(dict(x_bar))
                values,elements = get_results(len(design_space)-1, len(design_space), design_space, basefile, metric)
                fx_bar = min(values)
                if fx_bar < fl:
                    print "Re-aligning"
                    xl = dict(x_bar)
                    fl = fx_bar
                    j = 0
                else:
                    j = j+1
                print j
                if j== l:
                    print "Shrinking"
                    phi = c*phi; j =0
            flag = False
            #design_space.append(xopt)
            #fx_opt = get_results(len(design_space)-1, len(design_space), design_space, basefile)

            if fl < fx_opt:
                x_opt = dict(xl)
                fx_opt = fl # Is this necessary?
        print "Exploration Phase"
        x0 = generate_random(result,start,end,step,typ,relations,p,conf)
        while x0 in design_space:
            x0 = generate_random(result,start,end,step,typ,relations,p,conf) 
        design_space.append(dict(x0))
        values, elements = get_results(len(design_space)-1, len(design_space), design_space, basefile,metric)
        fx0 = min(values)
        if fx0 < f_gamma:
            print "New exploitation zone found"
            flag = True

        if fx0 < fx_opt:
            fx_opt = fx0 
            x_opt = dict(x0)

        if i==n:
            f_thresh.append(fx_opt)
            print f_thresh
            f_gamma = numpy.mean(numpy.array(f_thresh))
            i = 0
        i = i+1

    # print xi
    # design_space.append(dict(result))

def main():
    # python rrs.py conf.yaml rollingtopwords.yaml
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
    rrs(conf,sample,start,end,step,typ,relations,basefile,metric)

if __name__ == '__main__':
    main()

