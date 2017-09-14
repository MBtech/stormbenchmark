import yaml
import random
import math
import numpy
from collections import OrderedDict
import utils
import pandas
from pyDOE import *

def put_limits(value,start,end,step):
    if value<start:
        return start
    elif value>end:
        return end
    else:
        return utils.roundMultiple(value,step)

def step_func(result,start,end,step,typ,c,relations,change):
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

def get_sorted_results(metric):
    """
    Get the ranking of configurations that have been tested until now using the fitness function  (utility function)
    """
    cw = pandas.read_csv("numbers.csv")
    metric_values = list()
    for i in range(len(list(cw['no.']))):
        metric_values.append(utils.utility_function(metric, cw, i))
    
    return numpy.argsort(numpy.array(metric_values))

def get_parents(design_space, top, rand_select,metric):
    """
    Create a parent set comprising of high ranked parents and a few random parents (for diversity) and return two random parents from this set
    """
    indicies = get_sorted_results(metric)
    top_parents = int(math.ceil(len(indicies) * top))
    parent_set = list(indicies[0:top_parents])
    for i in indicies[top_parents:]:
        if numpy.random.choice([True, False],1, p=[rand_select, 1-rand_select]):
            parent_set.append(i)
    rand_parents = list(numpy.random.choice(parent_set, 2))
    return [design_space[rand_parents[1]],design_space[rand_parents[1]]]

def do_mutations(children, mu_rate, design_space, start, end, step, typ,relations, conf):
"""
Perform mutations based on the mutation probability
"""
    mutated_children  = list()
    for child in children:
        if numpy.random.choice([True, False], 1, p=[mu_rate , 1-mu_rate]):
            c = str(list(numpy.random.choice(conf, 1))[0])
            change = int(numpy.random.choice([1,-1],1))
            mu_child = step_func(child, start, end, step, typ, c, relations, change)
        else:
            mu_child = child
        mutated_children.append( mu_child)    
    return mutated_children

def do_crossover(parent_configs):
"""
Do the crossover between the two parents and get the child
"""
    child = dict() 
    for c in parent_configs[0].keys():
        if numpy.random.choice([True, False], 1):
            child[c] = parent_configs[1][c] 
        else: 
            child[c] = parent_configs[0][c]
    
    return [child]

def ga(conf,sample,start,end,step,typ, relations,basefile, metric):
    """
    Genetic algorithm for black box optimization
    :param conf: list of parameters to tune
    :param sample: a sample configuration
    :param start: the lower limit for values of each parameter (a dictionary)
    :param end: the upper limit for values of each parameter (a dictonary)
    :param step: the step size for values of each parameter (a dictionary)
    :param typ: type of the parameter (int,boolean etc) (Not being used currently)
    :param relations: defines the relationship that should be maintained between parameters that are being tuned and other parameters to maintain correctness of configuration
    :param basefile: basefile that contains a sample of a full topology configuration
    :param metric: comma separated metrics
    """
    # Initializations
    p =[]
    total_runs = 50
    m = 2 #initial sample size
    mu_rate = 0.1 # Mutation rate
    rand_select = 0.1 # Random parent selection rate
    top = 0.2 #Percentage of top parents selected 
    
    design_space = list()
    metric_values = list()
    numbers = list()
    result = dict(sample)
     
    # Generate the first n samples using LHS
    design_space = utils.generate_LHS(result,start,end,step,typ,relations,p,conf,m)
    # Get results and get the best configuration
    metric_values,numbers = utils.get_results(0,m,design_space, basefile,metric)

    while(len(design_space)<=total_runs):
	# Get two parents
        parents = get_parents(design_space, top, rand_select, metric)
        # Do a cross over between parents
        children = do_crossover(parents)
	# Perform mutations with a probability of 0.1
        mu_children = do_mutations(children, mu_rate, design_space, start, end, step, typ, relations, conf)
     
        # If the child has been evaluated before skip it
        if mu_children in design_space:
            continue
        n = len(design_space)
        design_space.extend(mu_children)
	# Get the results from running the application and based on the utility function
        metric_values, numbers = utils.get_results(n, n+len(mu_children), design_space, basefile, metric)

"""
The script requires 4 arguments
argument 1 is the configuration file that contains range of value for each parameter
argument 2 is a sample configuration file that will be used by the topology
argument 3 are the comma separeted metrics. Metric one could be latency currently supported types are lat_50, lat_60, lat_70, lat_80, lat_90, lat_99 and the second metric is the throughput constraint
argument 4 is the relations file that is used to change the buffer sizes according to the value of max.spout.pending parameter
"""
def main():
    # python ga.py conf.yaml rollingtopwords.yaml lat_90,throughput=100000 relations.yaml
    conf_file = sys.argv[1]
    basefile = sys.argv[2]
    metric = sys.argv[3]
    ref = open(conf_file, "r")
    sample = yaml.load(ref)
    result = dict(sample)
    start = dict(); end = dict(); step = dict(); typ = dict()
    ref = open(conf_file, "r")
    conf = utils.ordered_load(ref, yaml.SafeLoader).keys()
    print sample
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

    ga(conf,sample,start,end,step,typ,relations,basefile,metric)

if __name__ == '__main__':
    main()
