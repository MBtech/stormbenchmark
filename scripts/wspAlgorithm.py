#!/usr/bin/python2.7


import yaml
import random
import os
import math
import sys
from randomize import selective_write
import numpy
def distance(point1, point2):
    dist = 0
    for (k1,v1),(k2,v2) in zip(point1.items(),point2.items()):
        dist += math.pow(v1-v2,2)
    dist = math.sqrt(dist)

    return dist

def find_cov(design_space):
    mini = list()

    for i in range(len(design_space)):
        minimum = float('inf');

        for k in range(len(design_space)):
            if not(i==k):
                dist = distance(design_space[i],design_space[k])
                minimum = min(minimum,distance(design_space[i],design_space[k]))
        #print minimum
        mini.append(minimum)
    print sum(mini)
    mean_min = float(sum(mini))/float(len(mini))
    print len(mini)
    print mean_min
    final_cov = 0.0
    agg =0.0
    for i in range(len(mini)):
        agg += math.pow(mini[i]-mean_min,2)

    final_cov = math.sqrt(agg/len(mini))/mean_min
    print "The final Cov Value " + str(final_cov)




def wsp(design_space,dmin):
    rmarray = list()
    processed = list()
    
    #removing duplicates
    print "Size before duplicate removal "  + str(len(design_space))
    design_space = [dict(t) for t in set([tuple(d.items()) for d in design_space])]
    print "Size after duplicate removal "  + str(len(design_space))
    lowest_index = design_space[0]
    
    while len(processed)<(len(design_space)-len(rmarray)) :
        initial = lowest_index
        prev_lowest_index = lowest_index
        lowest = float('inf')
        #print "Next Iteration with lowest index " + str(lowest_index)
        #print processed
        count = 0
        for i in design_space[:]:
            count = count + 1 
            if not(i in processed) and not(i==prev_lowest_index):
                dist = distance(initial,i)

                if dist<dmin: 
                    #print "distance is "+ str(dist)
                    
                    #print design_space[i]
                    design_space.remove(i)
                elif dist>=dmin:
                    #print "Valid distance is "+ str(dist)
                    if lowest>dist:
                        #print "Lowest Index updated to " + str(i)
                        lowest_index = i
                        lowest = dist
            
            # elif i == lowest_index:
            #     print "Avoiding the same index"


        #print "Total times the loop was called " + str(count)
        processed.append(prev_lowest_index)
    print "total times rm array appended " + str(len(rmarray))
    print "total items in processed " + str(len(processed)) + " "+str(len(design_space))
    #print len(rmarray)
    for i in rmarray:
        try:
            #design_space = filter(lambda a: a != i, design_space)
            design_space.remove(i)
        except ValueError:
            print "Value not found", i
    find_cov(design_space)
    #print design_space
    print len(design_space)
    print "Printing final design space"
    
    write(design_space)

def write(design_space):
    if not os.path.exists("config_files"):
        os.makedirs("config_files")
    for i in range(len(design_space)):
        fname = "config_files/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname)
        
def generate_single(n):
    design = list()
    ref = open("conf.yaml", "r")
    sample = yaml.load(ref)
    result = dict(sample)
    for k in sample:
        # vrange = sample[k]
        # start = int(vrange.split(",")[0])
        # end = int(vrange.split(",")[1])
        # result[k] = random.randint(start,end)
        result[k] = n
    design.append(dict(result))
    write(design)

def generate_space():
    design_space = list()
    ref = open("conf.yaml", "r")
    sample = yaml.load(ref)
    for i in range(1024):
        #print sample
        
        result = dict(sample)
        for k in sample:
            vrange = sample[k]
            start = int(vrange.split(",")[0])
            step = int(vrange.split(",")[2])
            end = int(vrange.split(",")[1])
            print str(start)+" "+str(step)+ " "+str(end+1)
            result[k] = numpy.random.randint(start,end+1,step)
            #print k,result[k]
        #print result
        design_space.append(dict(result))
    
    wsp(design_space,16)

def main():
    if len(sys.argv)>1 and sys.argv[1]=='single':
        generate_single(int(sys.argv[2]))
    else:
        generate_space()
    

if __name__ == '__main__':
    main()
