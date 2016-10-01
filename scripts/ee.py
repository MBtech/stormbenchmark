import pandas as pd
import scipy.stats as stats
import yaml
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
import numpy as np
import itertools
import math
import operator
import sys
import plotly.graph_objs as go
import plotly.offline as plot
import os

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

# Number of files in config_files folder
path, dirs, files = os.walk("config_files").next()
file_count = len(files)
#print file_count

# Usage: python ee.py lat_90 example.csv
metric = sys.argv[1]
data_file = sys.argv[2]
conf_file = sys.argv[3]
cw = pd.read_csv(data_file)
design_space = list()
ref = open(conf_file, "r")
sample = yaml.load(ref)
result = dict(sample)
start = dict()
end = dict()
step = dict()
typ = dict()
ref = open(conf_file, "r")
conf = ordered_load(ref, yaml.SafeLoader).keys()
#conf = ["component.split_bolt_num","component.rolling_count_bolt_num","component.rank_bolt_num","component.spout_num","topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
#conf = ["component.rolling_count_bolt_num","component.split_bolt_num","component.spout_num","topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
absent = ["topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
alt = ["ackers","max_pending","receiver_threads","workers"]
alt2 = ["rolling_count","split"]
alt2.extend(alt)
metrics = ['lat_90','lat_80','lat_70','lat_60','lat_50','throughput']
#p = [4,4,4,4,4,3,3]
#p = [4,4,4,4,4,4,3,3,2,3,3,3,3,2,4,3]
p = [3,4,4,4,4,4,3,2,3,2,4,3]
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

index = 0
run = 0
r = 8
mu = dict().fromkeys(conf)
sigma = dict().fromkeys(conf)
mu_star = dict().fromkeys(conf)
d = [[]] * len(conf)
d[0] = [0]*r
#print d
for i in range(0,file_count-1,2):
    #print i
    if run>=r:
        index = index +1
        d[index] = [0]* r
        run = 0
    ref = open("config_files/test"+str(i)+".yaml","r")
    confs = yaml.load(ref)
    if max(cw[metric][i+1],cw[metric][i])>=2*min(cw[metric][i+1],cw[metric][i]):
        print "Suspricious run : " + str(i)
    if conf[index] in typ.keys():
        if typ[conf[index]]=="boolean":
            change = cw[metric][i+1] - cw[metric][i]/1.0
        else:
            change = cw[metric][i+1] - cw[metric][i]/1.0
            #change = cw[metric][i+1] - cw[metric][i]/((end[conf[index]]-start[conf[index]])/(p[index]-1))
    else:
        change = (cw[metric][i+1] - cw[metric][i])/1.0
    d[index][run] = change
    run = run +1
#print d 
index = 0
for i in d:
    cname = conf[index]
    mu[cname] = 0.0
    mu_star[cname] = 0.0
    for j in i:
        mu[cname] = mu[cname] + j
        mu_star[cname] = mu_star[cname] + abs(j)
    mu[cname] = mu[cname]/r
    mu_star[cname] = mu_star[cname]/r
    index = index +1 
#print sorted(mu.items(), key=operator.itemgetter(1), reverse=True)

m = sorted(mu_star.items(), key=operator.itemgetter(1), reverse=True)
print [x[0] for x in m]
index = 0
print m
for i in d:
    cname = conf[index]
    sigma[cname] = 0.0
    for j in i:
        sigma[cname] = math.pow(j-mu[cname],2) + sigma[cname]
    sigma[cname] = math.sqrt((1.0/(r-1))*sigma[cname])
    index = index + 1
#print sorted(sigma.items(), key=operator.itemgetter(1), reverse=True)

#print mu_star.keys()
#print mu_star.items()
# Create a trace
trace = go.Scatter(
    x = mu_star.keys(),
    y = mu_star.values()
)

data = [trace]

# Plot and embed in ipython notebook!
#plot.plot(data, filename=sys.argv[3])
