import random
import os
import math
import core_count

def generate_config(result,start,end,step,typ,p,conf):
    cores = core_count.get()
    count = 0
    for c in conf:
        if typ[c]=="threads":
            count += 1
    per_cat = math.floor(cores/count)
    for c in conf:
        if typ[c] =="threads":
            result[c] = per_cat

    return result

def max_select(conf,sample,start,end,step,typ, basefile):
    #p = [4,4,4,4,4,4,3,3]
    p = [4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
    design_space=list()
    result = dict(sample)
    result = generate_random(result,start,end,step,typ,p,conf)
    design_space.append(dict(result))
    print result
    write(design_space, basefile)
