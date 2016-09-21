import random
import core_count
import numpy as np
import numpy.random
from pyDOE import *
import math

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

# Generates the initial set of exploration points instead of using random points or LHS
def generate_initial(result,start,end,step,typ,relations,p,conf,size):
    cores = int(core_count.get())
    initial_space = list(dict())
    threads = np.random.dirichlet([5,5],size)
    lhm = lhs(len(conf),samples=size)
    for i in range(0,len(lhm)):
        sample = lhm[i]
        j =0
        print sample
        #print typ
        k=0
        for c in conf:
            if c in typ.keys():
                if typ[c] == "boolean":
                # FIXME: This shouldn't be a random choice
                    result[c] =  random.choice([True,False])
                else:
                    result[c] = pow(2,int(start[c] + (end[c] - start[c])*sample[j])) 
            else:
                if "component.spout" in c or "topology.acker" in c: 
                    result[c] = result["topology.workers"]
                elif "component" in c:
                    #Max so that it never gets below the min of one executor per bolt per worker 
                    result[c] = max(result["topology.workers"],roundDownMultiple((cores-1-2*int(result["topology.workers"]))*threads[i][k],result["topology.workers"]))
                    k+=1
                else:
                    result[c] = roundMultiple(int(start[c] + (end[c] - start[c])*sample[j]), step[c])
            if c in relations:
                for e in relations[c]:
                    result[e] = pow(2,int(math.ceil(math.log(result[c],2))))
                    if result[c]*1.1>result[e]:
                        result[e] = pow(2,int(math.ceil(math.log(result[c],2)))+1)
            j +=1
        initial_space.append(dict(result))
    return initial_space
