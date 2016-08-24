import yaml
import random
import os
import math
from randomize import selective_write
import sys
from collections import OrderedDict
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

def write(design_space ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(len(design_space)):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)


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
    ee_design(conf,sample,start,end,step,typ,basefile)

if __name__ == '__main__':
    main()
