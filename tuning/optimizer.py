import yaml
import os
import sys
import utils
from hc_lhs_stateful import hc
from hc_simple_improved import mhc
from rule_based import rule_based 

def main():
    # python rrs.py conf.yaml rollingtopwords.yaml lat_90 relations.yaml lat.yaml tp.yaml algorithm
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
    lat_file = sys.argv[5]
    ref = open(lat_file, "r")
    lat_conf = utils.ordered_load(ref, yaml.SafeLoader)
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
    tp_conf = utils.ordered_load(ref, yaml.SafeLoader)
    behav_tp = dict()
    tp_p = dict()
    i = 0
    for c in tp_conf.keys():
        tp_p[i] = c
        behav_tp[c] = int(tp_conf[c])
        i +=1
    print "Starting point is " + str(start)
    #print relations
    algo = sys.argv[7]
    if algo == "hc":
        hc(conf,sample,start,end,step,typ,relations,basefile,metric)
    elif algo == "mhc":
        mhc(conf,sample,start,end,step,typ,relations,basefile,metric)
    else:
	rule_based(conf,sample,start,end,step,typ,relations,basefile,metric,lat_p,tp_p,behav_tp,behav_lat)
if __name__ == '__main__':
    main()

