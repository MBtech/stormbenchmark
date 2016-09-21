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

# Generate a random points using LHS
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

# Generate a random points using LHS
def get_neighborhood(center,result,start,end,step,typ,p,conf,alpha):

    for c in conf:
        if c in typ.keys():
            if typ[c] == "exp":
                # New range of values for the conf c
                r = int((end[c] - start[c])*alpha)
                # So that it doesn't go below the already set low limit
                start[c] = roundMultiple(max(math.log(center[c],2)-round(r/2),start[c]),step[c])
                end[c] = roundMultiple(min(math.log(center[c],2)+round(r/2),end[c]),step[c])
        else:
            r = int((end[c] - start[c])*alpha)
            start[c] = roundMultiple(max(center[c]-round(r/2),start[c]),step[c])
            end[c] = roundMultiple(min(center[c]+round(r/2),end[c]),step[c])
    return start,end

# Get random point for the neighborhood of a point
def get_neighbor(result,start,end,step,typ,p,conf):
    xi = generate_random(result,start,end,step,typ,p,conf)
    while not in_neighborhood(result, xi, end, start, r, len(conf), step, typ, conf):
        xi = generate_random(result,start,end,step,typ,p,conf)
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
            metric_values.append(cw[metric][list(cw['no.']).index(i)])
        else:
           ret_array.append(i)
           metric_values.append(sys.maxint)
    
    print metric_values
    print ret_array
    return metric_values,ret_array

# Get the metric readings for all experiments
def get_metric(metric):
    cw = pandas.read_csv("numbers.csv")
    return list(cw[metric][:])

def get_pbest(design_space, numbers, conf, metric_values, start,end, step, typ, relations):
    vals = dict(list())
    y = dict(list())
    config = dict()
    for c in conf:
        vals[c] = list()
        y[c] = list()
        if c not in typ.keys():
            count =0
            for i in numbers:
                vals[c].append(design_space[i][c])
                y[c].append(metric_values[count])
                count+=1
        elif typ[c] == "exp":
            count =0
            for i in numbers:
                vals[c].append(design_space[i][c])
                y[c].append(metric_values[count])
                count+=1

    for c in conf:
        if c not in typ.keys():
            z = numpy.polyfit(vals[c], y[c], 2)
            f = numpy.poly1d(z)
            bounds = [start[c], end[c]]
            crit_points = bounds + [x for x in f.deriv().r if x.imag == 0 and bounds[0] < x.real < bounds[1]]
            crit_vals = [f(x) for x in crit_points]
            minimum = min(crit_vals)
            config[c] = roundMultiple(crit_points[crit_vals.index(minimum)],step[c])
        elif typ[c] == "exp":
            z = numpy.polyfit(vals[c], y[c], 2)
            f = numpy.poly1d(z)
            bounds = [start[c], end[c]]
            crit_points = bounds + [x for x in f.deriv().r if x.imag == 0 and bounds[0] < x.real < bounds[1]]
            crit_vals = [f(x) for x in crit_points]
            minimum = min(crit_vals)
            config[c] = pow(2,int(crit_points[crit_vals.index(minimum)]))
        if c in relations:
                for e in relations[c]:
                    config[e] = pow(2,int(math.ceil(math.log(config[c],2))))
                    if config[c]*1.1>config[e]:
                        config[e] = pow(2,int(math.ceil(math.log(config[c],2)))+1)
    return config

def hill_climbing(conf,sample,start,end,step,typ, relations,basefile, metric):
    # Initializations
    total_runs = 50
    m = 13
    #n = 2
    #l =2
    #m = int(0.1*total_runs) # Initial LHS sample size
    n = int(0.1*total_runs) # Number of local samples
    l = int(0.1*total_runs) # number of samples in the restart phase
    t = 0.4 # Threshold for neighborhood size
    alpha = 0.75 # The shrinking factor
    alpha_passed =alpha

    f_thresh = list()
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    #print result
    # Generate the first n samples using LHS
    design_space = generate_initial.generate_initial(result,start,end,step,typ,relations,p,conf,m)
    #print design_space
    # Get results and get the best configuration
    metric_values,numbers = get_results(0,m,design_space, basefile,metric)
    fx0 = min(metric_values)
    x0 = design_space[numbers[metric_values.index(fx0)]]

    n_start = dict(start)
    n_end = dict(end)
    local_search = True
    restart = True
    x_center = dict(x0)
    fx_center = fx0
    neighborhood_size = alpha_passed
    
    while restart:
        length = len(design_space)
        # Step 3: Local Search
        while local_search:
            print "Starting local search"
            # Generate samples from the local neighborhood of x0
            n_start, n_end = get_neighborhood(x0,result,n_start,n_end,step,typ,p,conf,alpha_passed)
            print n_start
            print n_end
            np = n
            new_configs = generate_LHS(result,n_start,n_end,step,typ,relations,p,conf,n)
            for config in new_configs:
                if config not in design_space:
                    design_space.append(config)
                else:
                    np -=1
            if np==0:
                print "No new configuration found. Going to restart phase"
                break;

            # Update the best configuration information
            metric_values,numbers = get_results(length,length+np,design_space, basefile,metric)
            fx_local = min(list(metric_values))
            x_local = design_space[numbers[metric_values.index(fx_local)]]
            if fx_local < fx0:
                x0 = dict(x_local)
                fx0 = fx_local

            length = len(design_space)
            # Get potentially the best configuration according to the local search
            x_new = get_pbest(design_space, numbers, conf, metric_values, n_start,n_end, step, typ,relations)
            design_space.append(x_new)
            val,num = get_results(length,length+1,design_space, basefile,metric)
            metric_values.extend(val)
            numbers.extend(num)
            length = len(design_space)

            fx_new = val
            if fx_new < fx0:
                print "New Best configuration found"
                x0 = dict(x_new)
                fx0 = fx_new
                alpha_passed =1
                neighborhood_size *= alpha_passed
                print "Best result: " + str(fx0)
                continue
            else:
                print "Next best configuration using quadratic equations"
                # Get potentially the best configuration according to the local search
                x_new = get_pbest(design_space, numbers, conf, metric_values, n_start,n_end, step, typ, relations)
                design_space.append(x_new)
                val,numbers = get_results(length,length+1,design_space, basefile,metric)
                fx_new = val
                length = len(design_space)
                if fx_new < fx0:
                    x0 = dict(x_new)
                    fx0 = fx_new
                    alpha_passed =1
                    neighborhood_size *= alpha_passed
                    print "Best result: " + str(fx0)
                    continue
                else:
                    print "Shrink phase"
                    alpha_passed = alpha
                    neighborhood_size *= alpha_passed
                    if neighborhood_size > t:
                        continue
                    else:
                        alpha_passed = alpha
                        neighborhood_size= alpha_passed
                        break

        print "Restart Phase"
        length = len(design_space)
        # Step 5: Restart phase
        design_space.extend(generate_LHS(result,start,end,step,typ,relations,p,conf,l))
        # Get results and get the best configuration
        metric_values,numbers = get_results(length,length+l,design_space, basefile,metric)
        length = len(design_space)

        fx_new = min(metric_values)
        x_new = design_space[numbers[metric_values.index(fx_new)]]
        n_start = dict(start)
        n_end = dict(end)
        values = get_metric(metric)
        if fx_new < numpy.percentile(values,80):
            if fx_new < fx0:
                fx0 = fx_new
                x0 = dict(x_new)
                print "Best result: " + str(fx0)
            if len(values)>=total_runs:
                break
            else:
                local_search = True
                continue
        else:
            if len(values)>=total_runs:
                break
            else:
                local_search = False
                continue
    print "Best configuration is " + str(x0)
    print "Best metric values is " + str(fx0)

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

    #print relations
    hill_climbing(conf,sample,start,end,step,typ,relations,basefile,metric)

if __name__ == '__main__':
    main()

