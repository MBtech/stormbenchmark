import pandas as pd
import scipy.stats as stats
import yaml
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from statsmodels.graphics.factorplots import interaction_plot
import matplotlib.pyplot as plt
import numpy as np
from pyvttbl import DataFrame
import itertools
import sys
import matplotlib.pyplot as plt

cw = pd.read_csv('numbers.csv')
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
for k in sample:
    vrange = sample[k]
    if len(vrange.split(","))==2:
        start[k] = int(vrange.split(",")[0])
        end[k] = int(vrange.split(",")[1])
    if len(vrange.split(","))==3:
	start[k] = int(vrange.split(",")[0])
	end[k] = int(vrange.split(",")[1])
        step[k] = int(vrange.split(",")[2])
config = dict()
for c in conf:
    config[c] = dict() 
    config[c]["low"] = list()
    config[c]["high"] = list()

for i in range(0,128):
    ref = open("config_files/test"+str(i)+".yaml","r")
    confs = yaml.load(ref)
    for c in conf:
        #print confs
        #print str(start[c])+ " " +str(confs[c])
        #print confs[c]
        if confs[c]==start[c]:
            config[c]["low"].append(i)
        else:
	    config[c]["high"].append(i)
        #print config[c]["low"]
groups = dict()
groups["low"] = list()
groups["high"] = list()
'''
for c in conf:
    for i in range(0,len(cw['90th latency'])):
        print cw[c][i]
        if cw[c][i] == start[c]:
            groups["low"].append(cw['90th latency'][i])
        else:
            groups["high"].append(cw['90th latency'][i])
'''
print config
for c in conf:
    index = 0
    for i in cw['test no.']:
        if i in config[c]["low"]:
            groups["low"].append(cw['90th latency'][index])
        else:
            groups["high"].append(cw['90th latency'][index])
        index = index + 1   
    #print groups
    print stats.f_oneway(groups["high"], groups["low"])


## Two way Anova 
datafile=sys.argv[1]
print sys.argv[1]
data = pd.read_csv(datafile)
df=DataFrame()
df.read_tbl(datafile)
index = 0
## Code for adding the independent variables that were not made part of the csv
for i in range(0,len(absent)): 
    data[alt[i]]=np.random.randn(len(data['no.']))
    df[alt[i]]=np.random.randn(len(df['no.']))
for i in data['no.']:
    ref = open("config_files/test"+str(i)+".yaml","r")
    confs = yaml.load(ref)
    for c,d in zip(absent,alt):
        data[d][index] = confs[c]
        df[d][index] = confs[c]
    index = index + 1 
## Model
calt = ["C("+item+")" for item in alt2]
lst = [":".join(items) for items in itertools.combinations(calt, r=2)]
#formula = 'lat_90~ C(rolling_count) + C(split) + C(ackers)+C(spout)+C(max_pending) + C(receiver_threads)+ C(workers)+ C(rolling_count):C(split)+C(receiver_threads):C(workers)+C(spout):C(max_pending)'
metrics = ['lat_90','lat_80','lat_70','lat_60','lat_50','throughput']
for m in metrics:
    print 'for '+ m
    formula = str(m)+'~' + ("+".join(calt))+"+"+str("+".join(lst))
#print data
    model = ols(formula, data).fit()
    aov_table = anova_lm(model, typ=2)
#eta_squared(aov_table)
#omega_squared(aov_table)
    #print(aov_table)
    print(aov_table['PR(>F)'])
    #aov_table['PR(>F)'].convert_objects(convert_numeric=True)
    #plt.plot(aov_table['PR(>F)'].convert_objects(convert_numeric=True))
    #plt.show()
df['id'] = xrange(len(df['lat_90']))
#print df 
#print(df.anova('lat_90', sub='no.', bfactors=['rolling_count', 'split']))#, 'ackers','spout','max_pending','receiver_threads','workers']))
#print cw['99th perct latency']
