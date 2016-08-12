import pandas as pd
import scipy.stats as stats
import yaml
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
import numpy as np
import itertools
cw = pd.read_csv('example2.csv')
#cw_lm=ols("'90th perct latency' ~  'threads rolling_count'", data=cw).fit() #Specify C for Categorical
#print(sm.stats.anova_lm(cw_lm, typ=2))
#print cw.columns`
design_space = list()
ref = open("conf.yaml", "r")
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
run =1
ref = open("config_files/test0.yaml","r")
confs0 = yaml.load(ref)
avg_change = [0] * len(conf)
for i in range(1,22):
    ref = open("config_files/test"+str(i)+".yaml","r")
    confs = yaml.load(ref)
    #print run,i
    print "Change in metric: " + str(abs(cw['lat_90'][run] - cw['lat_90'][0]))
    if cw['no.'][run]==i:
        change = abs(cw['lat_90'][run] - cw['lat_90'][0])/abs(confs[conf[index]]-confs0[conf[index]])
        avg_change[index] = change + avg_change[index]
        print "Partial derivative: " + str(change)
    run = run +1
    if i>=(index+1)*3:
        avg_change[index] = avg_change[index]/3
        print avg_change[index]
        index = index +1 
