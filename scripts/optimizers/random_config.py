import random
import os
import math

def generate_config(result,start,end,step,typ,p,conf):
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

def random_select(conf,sample,start,end,step,typ, basefile):
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    result = generate_random(result,start,end,step,typ,p,conf)
    design_space.append(dict(result))
    print result
    write(design_space, basefile)
