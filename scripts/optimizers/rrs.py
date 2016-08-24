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
    return num - (num % multiple)

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

# Get random point for the neighborhood of a point
def get_neighbor(result,start,end,step,typ,p,conf):
    xi = generate_random(result,start,end,step,typ,p,conf)
    while not in_neighborhood(result, xi, end, start, r, len(conf), step, typ, conf):
        xi = generate_random(result,start,end,step,typ,p,conf)
    return xi

# Get results for the specific configurations
def get_result(start_index, end_index, design_space, basefile):
    write(design_space, start,end ,basefile)
    for i in range(start, end):
        bashCommand = "./onescript.sh"
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]

    cw = pandas.read_csv("numbers.csv")
    return cw['lat_90'][start:end]

def rrs(conf,sample,start,end,step,typ, basefile, metric):
    # Initializations
    v=0.9
    p=0.99
    q=0.99
    r=0.1
    c=0.5
    st=0.01
    n=math.log(1-p)/math.log(1-r)
    l=math.log(1-q)/math.log(1-v)
    f_thresh = list()
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    # Generate the first n samples
    for i in range(0,n):
        result = generate_random(result,start,end,step,typ,p,conf)
        design_space.append(dict(result))


    # Get results and get the best configuration
    metric_values = get_results(0,n,design_space)
    fx0 = min(metric_values)
    x0 = design_space[metric_values.index(fx0)]
    f_gamma = fx0
    f_thresh.append(fx0)
    i =0
    flag = True
    xopt = dict(x0)

    # List for next n samples
    new_x = list(dict())
    new_fx = list()
    while len(design_space)=<120:
        if flag:
            j=0; fl = fx0; xl = dict(x0); phi=r
            while phi<st:
                x_bar = get_neighbor(xl,start,end,step,typ,p,conf)
                design_space.append(xbar)
                fx_bar = get_result(len(design_space)-1, len(design_space), design_space, basefile)
                if fx_bar < fl:
                    xl = dict(x_bar)
                    fl = fx_bar
                    j = 0
                else:
                    j = j+1

                if j = l:
                    phi = c*phi; j =0
            flag = False
            design_space.append(xopt)
            fx_opt = get_result(len(design_space)-1, len(design_space), design_space, basefile)

            if fl < fx_opt:
                x_opt = dict(xl)
                fx_opt = fl # Is this necessary?

        x0 = generate_random(result,start,end,step,typ,p,conf)
        design_space.append(dict(x0))
        fx0 = get_result(len(design_space)-1, len(design_space), design_space, basefile)

        new_x.append(x0)
        new_fx.append(fx0)
        if fx0 < f_gamma:
            flag = True

        if i=n:
            f_thresh.append(min(new_fx))
            f_gamma = mean(f_thresh)
            i = 0

        i = i+1

    # print xi
    # design_space.append(dict(result))

def main():
    # python generate_configs.py conf.yaml
    conf_file = sys.argv[1]
    basefile = sys.argv[2]
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
    rrs(conf,sample,start,end,step,typ,basefile,0)

if __name__ == '__main__':
    main()
