#!/usr/bin/python2.7

import yaml
import sys
import random
import os

def selective_write(select, fname):
    stream = open(os.path.expanduser('~')+"/.storm/rollingcount.yaml", "r")
    docs = yaml.load(stream)

    for k,v in docs.items():
        #x,y in refdoc.items()
        if k in select: 
            docs[k] = select[k]
        if isinstance(v, basestring):
            docs[k] = str(v)
    #print "\n"

    with open(fname, 'w') as yaml_file:
        yaml_file.write( yaml.dump(docs, default_flow_style=False))

def select_write(values, fname):
    stream = open(os.path.expanduser('~')+"/.storm/rollingcount.yaml", "r")
    docs = yaml.load(stream)

    for k,v in docs.items():
        #x,y in refdoc.items()
        for key in values:
            if key in k and 'topology' not in k and 'benchmark' not in k:
                print key
                docs[k] = values[key]
        if isinstance(v, basestring):
            docs[k] = str(v)
    #print "\n"
    print docs
    with open(fname, 'w') as yaml_file:
        yaml_file.write( yaml.dump(docs, default_flow_style=False))
