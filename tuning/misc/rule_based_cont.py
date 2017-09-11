import time
import yaml
import operator
import random
import os
import math
import numpy
import sys
import glob
from collections import OrderedDict
from utils import *
import itertools
import subprocess
import pandas
from pyDOE import *
import warnings
import pickle
import core_count
import json

# Continous Rule based optimization

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

def roundDownMultiple(number,multiple):
    num = number + 1
    if num % multiple == 0:
        return int(num - multiple)
    else:
        return int(num - (num % multiple))

def put_limits(value,start,end,step):
    if value<start:
        return start
    elif value>end:
        return end
    else:
        return roundMultiple(value,step)

def utility_function(metric,cw,i):
    tp_const =''
    with open('constraint') as f:
        tp_const = f.readline()
    metric = 'lat_90,throughput='+tp_const
    metrics = metric.split(',')
    limits = dict()
    improve_metric = ''
    for m in metrics:
        if '=' in m:
            limits[m.split('=')[0]] = int(m.split('=')[1])
        else:
            improve_metric = m
    for m in limits.keys():
        if cw[m][list(cw['no.']).index(i)]>=(limits[m]-(limits[m]*0.02)):
           return cw[improve_metric][list(cw['no.']).index(i)]
    return sys.maxint

def threshold_tp(metric):
    tp_const =0
    with open('constraint') as f:
        tp_const = f.readline()

    tp_const = int(tp_const)
    return tp_const

# Write the configuration to the file
# Can write multiple entries based on the start end end entries
def write(design_space, start,end ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(start,end):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)

def write_special(design_space,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    i = 500
    fname = folder+"/"+"run"+str(i)+".yaml"
    selective_write(design_space[0],fname, basefile)

def dict_product(dic):
    product = [x for x in apply(itertools.product, dic.values())]
    print product
    return [dict(zip(dic.keys(), p)) for p in product]

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

def get_tp(index):
    cw = pandas.read_csv("numbers.csv")
    metric = cw['throughput'][index]
    return metric
# Generates the initial set of exploration points instead of using random points or LHS
def generate_initial(result,start,end,step,typ,relations,p,conf):
    cores = int(core_count.get())
    threads = np.random.dirichlet([1000,1000],1)
    k=0
    print conf
    for c in conf:
        if c in typ.keys():
            if typ[c] == "boolean":
            # FIXME: This shouldn't be a random choice
                result[c] =  False
            else:
                result[c] = pow(2,int(start[c]))
        else:
            if "component.spout" in c or "topology.acker" in c:
                result[c] = result["topology.workers"]
            elif "component" in c:
                #Max so that it never gets below the min of one executor per bolt per worker
                result[c] = max(result["topology.workers"],roundDownMultiple((cores-1-3*int(result["topology.workers"]))*threads[0][k],result["topology.workers"]))
                k+=1
            else:
                result[c] = roundMultiple(int(start[c]),step[c])
        if c in relations:
            for e in relations[c]:
                result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
                if result[c]*1.1>result[e]:
                    result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
    print result
    return result

def step_func(result,start,end,step,typ,p,c,relations,change):
    print "Changing " + str(c)
    if c in typ.keys():
        if typ[c] == "boolean":
            if change==-1: result[c]=False;
            else: result[c]=True;
        if typ[c] =="exp":
            result[c] = pow(2,put_limits(math.log(result[c],2) + ((change)*(step[c]))),start[c],end[c],step[c])
    else:
        result[c] = put_limits(result[c] + ((change)*(step[c])),start[c],end[c],step[c])
    print "New value for " + str(c) + " = " + str(result[c])
    if c in relations:
        for e in relations[c]:
            result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
            if result[c]*1.1>result[e]:
                result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
    return result

def getstats(index):
    direc = "json_files/"
    tuples = dict()
    latency = dict()
    processtime = dict()
    bolts = dict()
    capacity = dict()
    with open(direc+'topology'+str(index)+'.json') as data_file:
        data = json.load(data_file)
        for i in data['bolts']:
            tuples[i['boltId']] = int(i['acked'])
            latency[i['boltId']] = float(i['processLatency'])
            capacity[i['boltId']] = float(i['capacity'])
    return capacity

def adjust(capacity,result,bolt_ids,step):
    print step
    maxcap_bolt = max(capacity.iteritems(), key=operator.itemgetter(1))[0]
    for i in capacity:
        for k in bolt_ids:
            if i in k:
                result[k] = roundDownMultiple(int(result[k]) * (capacity[i]/capacity[maxcap_bolt]), step[k])
    print result
    return result

def shift(capacity,result,bolt_ids,step):
    residual = 0
    maxcap_bolt = max(capacity.iteritems(), key=operator.itemgetter(1))[0]
    for k in bolt_ids:
        if maxcap_bolt not in k:
            result[k] = roundDownMultiple(int(result[k])-int(step[k]), step[k]) 
            residual += int(step[k])   
    for k in bolt_ids:
        if maxcap_bolt in k:
            result[k] = roundDownMultiple(int(result[k])+residual, step[k])
    print result
    return result

def adjustmax(capacity,result,bolt_ids,step):
    print step
    bolts = dict()
    for k in bolt_ids:
        bolts[k]=int(result[k])
    
    max_bolt = max(bolts.iteritems(), key=operator.itemgetter(1))[0]
    result[max_bolt] = roundDownMultiple(int(result[max_bolt])-int(step[max_bolt]), step[max_bolt])
    print result
    return result

def downgrade(capacity,result,bolt_ids,step):
    maxcap_bolt = max(capacity.iteritems(), key=operator.itemgetter(1))[0]
    #cap = min(capacity[maxcap_bolt]+0.2,1.0)
    cap = capacity[maxcap_bolt]
    print "Max Capacity is at " + str(cap)
    for k in bolt_ids:
        if maxcap_bolt in k:
            result[k] = roundDownMultiple(int(result[k]) - step[k], step[k])
    print result
    return result

def upgrade(capacity,result,bolt_ids,step):
    maxcap_bolt = max(capacity.iteritems(), key=operator.itemgetter(1))[0]
    #cap = min(capacity[maxcap_bolt]+0.2,1.0)
    cap = capacity[maxcap_bolt]
    print "Max Capacity is at " + str(cap)
    for k in bolt_ids:
        if maxcap_bolt in k:
            result[k] = roundDownMultiple(int(result[k]) + step[k], step[k])
    print result
    return result

def checkit(c,x0,start,end,change):
    if x0[c]==False and change ==-1:
        return False
    elif x0[c]==True and change == 1:
        return False
    elif x0[c]==start[c] and change==-1:
        return False
    elif x0[c]==end[c] and change==1:
        return False
    else:
        return True

def run_best(start, end, design_space, basefile):
    write_special(design_space,basefile)
    bashCommand = "./runbest.sh " + str(500) + " &"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    print "finished start the process"

def find_best(f,tp_const):
    data = pandas.read_csv(f)
    filtered = data[data['throughput']>=tp_const]
    if not filtered.empty:
        lat = list(filtered['lat_90'])
        print min(lat)
        i = filtered[filtered['lat_90']==min(lat)]['no.'].iloc[0]
        r = open('config_files/test'+str(i)+'.yaml', 'r')
        config = dict(yaml.load(r))
        return config,i
    else:
        return None,None

def find_init(f,tp_const):
    data = pandas.read_csv(f)
    max_tp = max(list(data['throughput'])) 
    i = list(data['throughput']).index(max_tp)
    r = open('config_files/test'+str(i)+'.yaml', 'r')
    config = dict(yaml.load(r))
    return config,i

def continuation(conf,sample,start,end,step,typ, relations,basefile, metric,lat_p,tp_p,behav_tp,behav_lat):
    #Read current constraints
    tp = 0
    with open('constraint') as f:
        tp_const = f.readline()
    tp_const = int(tp_const)
    print tp_const
    #Read the throughput numbers
    newest = max(glob.iglob('reports/*.csv'), key=os.path.getmtime)
    csvfile = newest
    print csvfile
    ps = subprocess.Popen(('tail', '-n10',csvfile), stdout=subprocess.PIPE)
    output = subprocess.check_output(('awk', '-F,','{print $12}'), stdin=ps.stdout)
    current_tp =  numpy.mean([int(x) for x in output.splitlines()])

    print current_tp
    #If constraint is met return
    if current_tp>=tp_const:
        return False,None,None

    #If constraint isn't met, initiate the optimization.
    else:
        f = 'numbers.csv'
        x,i = find_best(f,tp_const)
        if x!=None:
            return True,x,i
        else:
            ini = False
            config,i=find_init(f,tp_const)
            x,i =rule_based(conf,sample,start,end,step,typ, relations,basefile, metric,lat_p,tp_p,behav_tp,behav_lat,ini,config)
            return True,x,i

def get_design_space():
    design_space = list(dict())
    for f in glob.iglob('config_files/test*.yaml'):
        r = open(f,'r')
        config = dict(yaml.load(r))
        design_space.append(config) 
    return design_space

	
def rule_based(conf,sample,start,end,step,typ, relations,basefile, metric,lat_p,tp_p,behav_tp,behav_lat,ini,config):

	    #steps
    #Step 1
    #Generate initial configuration
    p = []
    design_space = list(dict())
    if ini:
        config = generate_initial(dict(sample),start,end,step,typ,relations,p,conf)
    else:
        design_space = get_design_space()
    length = len(design_space)
    design_space.append(config)
    metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
    fx0 = min(metric_values)
    x0 = dict(config)
    lastx0 = dict(config)
    lastfx0 = fx0
    index = 0
    f = True
    retry = False
    bolt_ids = ["component.rolling_count_bolt_num","component.split_bolt_num" ]
    #bolt_ids = ["component.rolling_count_bolt_num","component.split_bolt_num","component.rank_bolt_num"]
    #bolt_ids = ["component.stemming_bolt_num","component.positive_bolt_num","component.negative_bolt_num","component.score_bolt_num","component.logging_bolt_num"]
    while f:
        print f
        max_capacity = 0
        print retry

        met = dict()
        if fx0==sys.maxint:
            print "Current constraint is throughput"
            constraint = "throughput"
            met = dict(tp_p)
            change = dict(behav_tp)
        else:
            print "Current constraint is latency"
            constraint = "latency"
            met = dict(lat_p)
            change = dict(behav_lat)
        priority = 0
        t = 6
        discontinue=False

        print "Best configuration " + str(x0)
        tp_old = get_tp(length)
        print tp_old
        
        while constraint == "throughput":
            x_new = dict(x0)
            priority = 0
            while priority<t:
                c = met[priority]
                x_new = step_func(dict(x_new),start,end,step,typ,p,c,relations,change[met[priority]])
                priority += 1
            if x_new in design_space:
                print x_new
                print design_space
                discontinue=True
                break
            print "New Configuration " + str(x_new)
            length = len(design_space)
            design_space.append(x_new)
            metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
            fx_new = min(metric_values)
            tp = get_tp(length)
            thres_tp = threshold_tp(metric) + (threshold_tp(metric)*0.02)
            if constraint=="throughput" and tp>tp_old and fx_new<=fx0:
                fx0 = fx_new
                x0 = dict(x_new)
            elif fx_new!=sys.maxint:     
                print "Changing current constraint to latency"
                constraint = "latency"
                met = dict(lat_p)
                change = dict(behav_lat)
                priority = 0
                retry = True
                break
            tp_old = tp
        if discontinue:
            break
        while priority<t:
            print met[priority]
            print change[met[priority]]
            print x0
            c = met[priority]

            check = checkit(c,x0,start,end,change[met[priority]])
            if not check:
                priority+=1
                if priority == t and constraint=="latency":
                    f = False
                    break
                continue

            x_new = step_func(dict(x0),start,end,step,typ,p,c,relations,change[met[priority]])
            length = len(design_space)
            design_space.append(x_new)
            metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
            fx_new = min(metric_values)
            tp = get_tp(length)
            thres_tp = threshold_tp(metric) + (threshold_tp(metric)*0.1)
            if fx_new<fx0:
                fx0 = fx_new
                x0 = dict(x_new)
                continue
            elif constraint=="throughput" and tp>tp_old and fx_new<=fx0:
                fx0 = fx_new
                x0 = dict(x_new)
            else:
                priority += 1
            if (priority == t and constraint=="throughput") or (constraint=="throughput" and fx_new!=sys.maxint and tp>=thres_tp):
                print "Changing current constraint to latency"
                constraint = "latency"
                met = dict(lat_p)
                change = dict(behav_lat)
                priority = 0
                retry = True
                break
            if priority == t and constraint=="latency":
                f = False
                break
            tp_old = tp
        
        
        retry = False
        
        x_best = dict(x0)
        fx_best = fx0
        capacity = getstats(index)
        max_capacity = max(capacity.iteritems(), key=operator.itemgetter(1))[1]
        print "Max Capacity is at " + str(max_capacity)

        while max_capacity<0.8:
            print "Increasing utilization"
            length = len(design_space)
            #capacity = getstats(index)
            #max_capacity = max(capacity.iteritems(), key=operator.itemgetter(1))[1]
            config = downgrade(capacity,dict(x_best),bolt_ids,step)
            design_space.append(config)
            metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
            fx_new = min(metric_values)
            x_new = dict(config)
            if fx_new==sys.maxint:
                break
            elif not retry and fx_new>fx0:
                index +=1
                fx_best= fx_new
                x_best = dict(x_new)
            elif fx_new<=fx0:
                index+=1
                fx_best = fx_new
                x_best = dict(x_new)
                fx0 = fx_new
                x0 = dict(x_new)
            elif fx_new>fx0:
                break
            capacity = getstats(index)
            max_capacity = max(capacity.iteritems(), key=operator.itemgetter(1))[1]
            print "Max Capacity is at " + str(max_capacity)
        print "Adjusting the thread distribution"
        length = len(design_space)
        print index
        capacity = getstats(index)
        config = adjust(capacity,dict(x_best),bolt_ids,step)
        design_space.append(config)
        metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
        fx_new = min(metric_values)
        x_new = dict(config)
        #if not retry or fx_new<fx0:
        #    fx0 = fx_new
        #    x0 = dict(x_new)
        cont = False
        if not retry and fx_new>fx0 and fx_new!=sys.maxint:
                index +=1
                fx_best= fx_new
                x_best = dict(x_new)
        elif fx_new<=fx0:
                index+=1
                fx_best = fx_new
                x_best = dict(x_new)
                fx0 = fx_new
                x0 = dict(x_new)
        if fx_new==sys.maxint:
                cont = True
        else:
                cont = False
        retry = True
       
        print cont  
        while cont:
            length = len(design_space)
            capacity=getstats(index)
            config  = shift(capacity,dict(x_best),bolt_ids,step)
            design_space.append(config)
            metric_values,numbers = get_results(length,length+1,design_space, basefile,metric)
            fx_new = min(metric_values)
            x_new = dict(config) 
            if fx_new == sys.maxint:
                break
            elif fx_new>fx0:
                index+=1
                fx_best = fx_new
                x_best = dict(x_new)
            elif fx_new<=fx0:
                index+=1
                fx_best = fx_new
                x_best = dict(x_new)
                fx0 = fx_new
                x0 = dict(x_new)       
    
    print "Best configuration is " + str(x0)
    print "Best metric values is " + str(fx0)
    print "Best configuration is with low resource usage" + str(x_best)
    print "Best metric values is with low resource usage" + str(fx_best)
    if fx_best*0.9<=fx0:
        return x_best,length
    else:
        return x0,length


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
    lat_file = sys.argv[5]
    ref = open(lat_file, "r")
    lat_conf = ordered_load(ref, yaml.SafeLoader)
    lat_p = dict()
    behav_lat = dict()
    i = 0
    print lat_conf
    for c in lat_conf.keys():
        lat_p[i] = c
        print lat_conf[c]
        behav_lat[c] = int(lat_conf[c])
        i +=1
    tp_file = sys.argv[6]
    ref = open(tp_file, "r")
    tp_conf = ordered_load(ref, yaml.SafeLoader)
    behav_tp = dict()
    tp_p = dict()
    i = 0
    for c in tp_conf.keys():
        tp_p[i] = c
        behav_tp[c] = int(tp_conf[c])
        i +=1
    print "Starting point is " + str(start)
    #print relations
    tp_const =0
    with open('constraint') as f:
        tp_const = f.readline()
    tp_const = int(tp_const)
    print tp_const
    f = '/home/ubuntu/bilal/storm-benchmark/tuning/numbers.csv'
    if not os.path.isfile(f):
        print "File doesn't exist. Running rule based aproach"
        x,i = rule_based(conf,sample,start,end,step,typ,relations,basefile,metric,lat_p,tp_p,behav_tp,behav_lat,True,dict())
    else:
        print "File exists, selecting the best one"
        x,i = find_best(f,tp_const)
    print "New configuration found: " + str(x)
    run_best(i, i+1, [x], basefile)
    time.sleep(200)
    while True:
        flag, x,i = continuation(conf,sample,start,end,step,typ,relations,basefile,metric,lat_p,tp_p,behav_tp,behav_lat)
        if flag:
            print "New configuration found: " + str(x)
            run_best(i, i+1, [x], basefile)
            time.sleep(200)
        else:
            print "Gonna sleep"
            time.sleep(10)

if __name__ == '__main__':
    main()

