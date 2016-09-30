import subprocess
from randomize import selective_write
import pandas
import sys
import random
import math
import os

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
