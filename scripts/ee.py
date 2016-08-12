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
metric = sys.argv[1]
cw = pd.read_csv('example3.csv')
design_space = list()
ref = open("conf1.yaml", "r")
sample = yaml.load(ref)
result = dict(sample)
start = dict()
end = dict()
step = dict()
conf = ["component.rolling_count_bolt_num","component.split_bolt_num","component.spout_num","topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
absent = ["topology.acker.executors","topology.max.spout.pending","topology.worker.receiver.thread.count","topology.workers"]
alt = ["ackers","max_pending","receiver_threads","workers"]
alt2 = ["rolling_count","split"]
alt2.extend(alt)
metrics = ['lat_90','lat_80','lat_70','lat_60','lat_50','throughput']
p = [4,4,4,4,4,3,3]
for k in sample:
    vrange = sample[k]
    if len(vrange.split(","))==2:
        start[k] = int(vrange.split(",")[0])
        end[k] = int(vrange.split(",")[1])
    if len(vrange.split(","))==3:
	start[k] = int(vrange.split(",")[0])
	end[k] = int(vrange.split(",")[1])
        step[k] = int(vrange.split(",")[2])

index = 0
run = 0
r = 4
mu = dict().fromkeys(conf)
sigma = dict().fromkeys(conf)
mu_star = dict().fromkeys(conf)
d = [[]] * len(conf)
d[0] = [0]*r
print d
for i in range(0,56,2):
    if run>=r:
        index = index +1
        d[index] = [0]* r
        run = 0
    ref = open("config_files/test"+str(i)+".yaml","r")
    confs = yaml.load(ref)
    change = cw[metric][i+1] - cw[metric][i]/((end[conf[index]]-start[conf[index]])/(p[index]-1))
    d[index][run] = change
    run = run +1

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
print sorted(mu.items(), key=operator.itemgetter(1), reverse=True)
print sorted(mu_star.items(), key=operator.itemgetter(1), reverse=True)

index = 0
for i in d:
    cname = conf[index]
    sigma[cname] = 0.0
    for j in i:
        sigma[cname] = math.pow(j-mu[cname],2) + sigma[cname]
    sigma[cname] = math.sqrt((1.0/(r-1))*sigma[cname])
    index = index + 1
print sorted(sigma.items(), key=operator.itemgetter(1), reverse=True)

print mu_star.keys()
print mu_star.items()
# Create a trace
trace = go.Scatter(
    x = mu.keys(),
    y = mu.values()
)

data = [trace]

# Plot and embed in ipython notebook!
plot.plot(data, filename=sys.argv[2])
