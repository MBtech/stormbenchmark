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
from pyDOE import *
import warnings
import generate_initial
import pickle
import utils


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
                result[c] = utils.roundMultiple(int(start[c] + (end[c] - start[c])*sample[j]), step[c])
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
                start[c] = utils.roundMultiple(max(math.log(center[c],2)-round(r/2),start[c]),step[c])
                end[c] = utils.roundMultiple(min(math.log(center[c],2)+round(r/2),end[c]),step[c])
        else:
            r = int((end[c] - start[c])*alpha)
            start[c] = utils.roundMultiple(max(center[c]-round(r/2),start[c]),step[c])
            end[c] = utils.roundMultiple(min(center[c]+round(r/2),end[c]),step[c])
    return start,end

# Get random point for the neighborhood of a point
def get_neighbor(result,start,end,step,typ,p,conf):
    xi = utils.generate_random(result,start,end,step,typ,p,conf)
    while not in_neighborhood(result, xi, end, start, r, len(conf), step, typ, conf):
        xi = utils.generate_random(result,start,end,step,typ,p,conf)
    return xi

# Get results for the specific configurations
def get_results(start, end, design_space, basefile, metric):
    utils.write(design_space, start,end ,basefile)
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
            metric_values.append(utils.utility_function(metric,cw,i))
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
        metrics.append(utils.utility_function(metric,cw,i))
    return metrics

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
            config[c] = utils.roundMultiple(crit_points[crit_vals.index(minimum)],step[c])
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

def initial_sample():
    design_space = generate_initial.generate_initial(result,start,end,step,typ,relations,p,conf,m)
    metric_values,numbers = get_results(0,m,design_space, basefile,metric)
    fx0 = min(metric_values)
    x0 = design_space[numbers[metric_values.index(fx0)]]


def local_search():
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

    # Update the best configuration information
    metric_values,numbers = get_results(length,length+np,design_space, basefile,metric)
    fx_local = min(list(metric_values))
    x_local = design_space[numbers[metric_values.index(fx_local)]]

def change_best():
    x0 = dict(x_local)
    fx0 = fx_local

def restart_phase(design_space,result,start,end,step,typ,relations,p,conf,l,length, basefile,metric):
    design_space.extend(generate_LHS(result,start,end,step,typ,relations,p,conf,l))
    # Get results and get the best configuration
    metric_values,numbers = get_results(length,length+l,design_space, basefile,metric)
    return design_space,metric_values,numbers

def if_less(x,y):
    return x<y

def post_restart_phase(values,fx_new,x_new,fx0,x0, length, design_space,basefile,metric,
                      metric_values,numbers,total_runs):
    if fx_new < numpy.percentile(values,80):
        if fx_new < fx0:
            print "Potential best result"
            design_space.append(x_new)
            value,number = get_results(length,length+1,design_space, basefile,metric)
            metric_values.extend(value)
            numbers.extend(number)
            f_temp = min(list(value))
            x_temp =  dict(x_new)
            #f_temp = min(metric_values)
            #x_temp = design_space[numbers[metric_values.index(f_temp)]]
            length = len(design_space)

            if f_temp < fx0:
                if fx_new < f_temp:
                    x0 = dict(x_temp)
                    fx0 = f_temp
                else:
                    x0 = dict(x_new)
                    fx0 = fx_new
                print "Best result: " + str(fx0)
        if len(values)>=total_runs:
            return "terminate",design_space,metric_values,numbers,fx0,x0,length
        else:
            local_search = True
            return "localsearch",design_space,metric_values,numbers,fx0,x0,length
    else:
        if len(values)>=total_runs:
            return "terminate",design_space,metric_values,numbers,fx0,x0,length
        else:
            local_search = False
            return "restart",design_space,metric_values,numbers,fx0,x0,length
    return fx0,x0,design_space,metric_values,numbers
def confirm(fx0,x0,design_space,basefile,metric,n1,n2,metric_values,numbers):
    design_space.append(x0)
    value,number = get_results(n1,n2,design_space, basefile,metric)
    metric_values.extend(value)
    numbers.extend(number)
    fx_temp = min(list(value))
    m = list(metric_values)
    if fx_temp > fx0:
        m.remove(min(metric_values))
        second = min(m)
        print second
        fx0 = second
        x0 = design_space[numbers[metric_values.index(fx0)]]
    print "Best performance "+str(fx0)
    #fx0 = min(metric_values)
    #x0 = design_space[numbers[metric_values.index(fx0)]]
    return fx0,x0,design_space,metric_values,numbers

#def confirm(x0,design_space,basefile,metric,n1,n2,metric_values,numbers):
#    design_space.append(x0)
#    value,number = get_results(n1,n2,design_space, basefile,metric)
#    metric_values.extend(value)
#    numbers.extend(number)
#    fx0 = min(metric_values)
#    x0 = design_space[numbers[metric_values.index(fx0)]]
#    return fx0,x0,design_space,metric_values,numbers

def new_best(fx0,x0,fx_local,x_local,length,np,design_space,basefile,metric,metric_values,numbers):
    if fx_local < fx0:
        print "Potential best result"
        design_space.append(x_local)
        value,number = get_results(length+np,length+np+1,design_space, basefile,metric)
        metric_values.extend(value)
        numbers.extend(number)
        f_temp = min(list(value))
        x_temp =  dict(x_local)
        #f_temp = min(metric_values)
        #x_temp = design_space[numbers[metric_values.index(f_temp)]]
        print "Verifying"
        if f_temp < fx0:
            print "New Best configuration found"
            if fx_local < f_temp:
                x0 = dict(x_temp)
                fx0 = f_temp
            else:
                x0 = dict(x_local)
                fx0 = fx_local 
    return fx0,x0,design_space,metric_values,numbers

def new_potentialbest(fx0,x0,design_space,length,basefile,metric,metric_values,numbers,neighborhood_size,alpha_passed,conf,n_start,n_end, step, typ,relations):
    x_new = get_pbest(design_space, numbers, conf, metric_values, n_start,n_end, step, typ,relations)
    design_space.append(x_new)
    val,num = get_results(length,length+1,design_space, basefile,metric)
    metric_values.extend(val)
    numbers.extend(num)
    fx_new = val
    length = len(design_space)
    if fx_new < fx0:
        print "Potential best result"
        design_space.append(x_new)
        value,number = get_results(length,length+1,design_space, basefile,metric)
        metric_values.extend(value)
        numbers.extend(number)
        f_temp = min(list(value))
        x_temp =  dict(x_new)
        #f_temp = min(metric_values)
        #x_temp = design_space[numbers[metric_values.index(f_temp)]]
        length = len(design_space)

        if f_temp < fx0:
            print "New Best configuration found"
            if fx_new < f_temp:
                x0 = dict(x_temp)
                fx0 = f_temp
            else:
                x0 = dict(x_new)
                fx0 = fx_new
            alpha_passed =1
            neighborhood_size *= alpha_passed
            print "Best result: " + str(fx0)
            return True,x0,fx0,alpha_passed,neighborhood_size,length

    return False,x0,fx0,alpha_passed,neighborhood_size,length

def mhc(conf,sample,start,end,step,typ, relations,basefile, metric):

    # Initializations
    p =[]
    total_runs = 50
    m = 12
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
    if os.path.isfile(filename):
        f_read = open(filename,'r')
        program_state = pickle.load(f_read)
        x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size = program_state
        f_data = open('numbers.csv','r')
        lines = f_data.readlines()
        f_data.close()
        f_data = open('numbers.csv','w')
        for line in lines[0:len(design_space)+1]:
            f_data.write(line)
        f_data.close()
        f_read.close()
        print "Best configuration after restore is"
        print fx0
        print "Size of design space " + str(len(design_space))
        print "Resume state is " + state
    else:
        f_thresh = list()
        # Generate the first n samples using LHS
	print result
        design_space = generate_initial.generate_initial(result,start,end,step,typ,relations,[],conf,m)
        # Get results and get the best configuration
        metric_values,numbers = get_results(0,m,design_space, basefile,metric)
        fx0 = min(metric_values)
        x0 = design_space[numbers[metric_values.index(fx0)]]
        fx0,x0,design_space,metric_values,numbers = confirm(fx0,x0,design_space,basefile,metric,m,m+1,metric_values,numbers)
        n_start = dict(start)
        n_end = dict(end)
        local_search = True
        restart = True
        x_center = dict(x0)
        fx_center = fx0
        state = "localsearch"

        f = open(filename,'wb')
        program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
        pickle.dump(program_state,f)
        f.close()

    length = len(design_space)
    while True:
        length = len(design_space)
        if state == "localsearch":
            np = n
            print "Starting local search"
            # Generate samples from the local neighborhood of x0
            n_start, n_end = get_neighborhood(x0,result,n_start,n_end,step,typ,p,conf,alpha_passed)
            print n_start
            print n_end
            new_configs = generate_LHS(result,n_start,n_end,step,typ,relations,p,conf,n)
            for config in new_configs:
                if config not in design_space:
                    design_space.append(config)
                else:
                    np -=1
            if np==0:
                print "No new configuration found. Going to restart phase"
                state = "restart"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
            else:
                # Update the best configuration information
                print np, length
                metric_values,numbers = get_results(length,length+np,design_space, basefile,metric)
                fx_local = min(list(metric_values))
                x_local = design_space[numbers[metric_values.index(fx_local)]]

                # Check if new configuration is a better one
                fx0,x0,design_space,metric_values,numbers = new_best(fx0,x0,fx_local,x_local,length,np,design_space,basefile,metric,metric_values,numbers)
                length = len(design_space)
                state = "pbest1"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
        elif state == "pbest1":
            # Get potentially the best configuration according to the local search
            cont,x0,fx0,alpha_passed,neighborhood_size,length =new_potentialbest(fx0,x0,design_space,length,basefile,metric,metric_values,numbers,neighborhood_size,alpha_passed,conf,n_start,n_end, step, typ,relations)
            if cont:
                state = "localsearch"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
            else:
                state = "pbest2"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
        elif state == "pbest2":
            # Get potentially the best configuration according to the local search
            cont,x0,fx0,alpha_passed,neighborhood_size,length =new_potentialbest(fx0,x0,design_space,length,basefile,metric,metric_values,numbers,neighborhood_size,alpha_passed,conf,n_start,n_end, step, typ,relations)
            if cont:
                state = "localsearch"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
            else:
                state = "shrink"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()

        elif state == "shrink":
            print "Shrink phase"
            alpha_passed = alpha
            neighborhood_size *= alpha_passed
            if neighborhood_size > t:
                state = "localsearch"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()
            else:
                alpha_passed = alpha
                neighborhood_size= alpha_passed
                state = "restart"
                f = open(filename,'wb')
                program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
                pickle.dump(program_state,f)
                f.close()

        elif state == "restart":
            print "Restart Phase"
            length = len(design_space)
            # Step 5: Restart phase
            design_space,metric_values,numbers = restart_phase(design_space,result,start,end,step,typ,relations,p,conf,l,length, basefile,metric)
            print metric_values
            fx_new = min(metric_values)
            x_new = design_space[numbers[metric_values.index(fx_new)]]
            print numbers[metric_values.index(fx_new)]
            n_start = dict(start)
            n_end = dict(end)
            values = get_metric(metric)
            state,design_space,metric_values,numbers,fx0,x0,length = post_restart_phase(values,fx_new,x_new,fx0,x0, length, design_space,basefile,metric,metric_values,numbers,total_runs)
            f = open(filename,'wb')
            program_state = [x0,fx0,state,design_space,metric_values,numbers,n_start,n_end,alpha_passed,np,neighborhood_size]
            pickle.dump(program_state,f)
            f.close()
        else:
            break

    print "Best configuration is " + str(x0)
    print "Best metric values is " + str(fx0)



