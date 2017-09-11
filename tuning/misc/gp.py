import pylab as pb
pb.ion()
import numpy as np
import GPy
import math
import yaml
import sys
import pandas
import utils


# sample inputs and outputs
X = np.random.uniform(0.,10.,(50,2))
Y = abs(X[:,0:1] * X[:,1:2])+np.random.randn(50,1)*0.05
print X
print Y

conf_file = sys.argv[1]
ref = open(conf_file, "r")
sample = yaml.load(ref)
result = dict(sample)
vals = dict(list())
cw = pandas.read_csv("numbers.csv")

for c in result:
    vals[c] = list()
print result
total_runs = 120
values = [[]]*total_runs
Y =[[]]*total_runs
print values
vals["output"] = list()
for i in range(0,total_runs):
    cfile = "config_files/test"+str(i)+".yaml"
    ref = open(cfile,"r")
    conf = dict(yaml.load(ref))
    Y[i] = list()
    values[i] = list()
    for c in result:
        if c in conf:
            vals[c].append(conf[c])
            values[i].append(conf[c])
    vals["output"].append(cw['lat_90'][i])   
    Y[i].append(cw['lat_90'][i])
print values
#print Y
V = list()
V = np.array(values)
print V.shape
X = np.array(values)
Y = np.array(Y)
print Y
print X
# define kernel
kernel = GPy.kern.RBF(input_dim=12, variance=1., lengthscale=1.)
# create simple GP model
m = GPy.models.GPRegression(X,Y,kernel)

# contrain all parameters to be positive
#m.constrain_positive('')

# optimize and plot
m.optimize(max_iters=100)
values = [[]] * 30
metrics = list()
for i in range(total_runs,total_runs+30):
    cfile = "config_files/test"+str(i)+".yaml"
    ref = open(cfile,"r")
    conf = dict(yaml.load(ref))
    values[i-120] = list()
    for c in result:
        if c in conf:
            values[i-120].append(conf[c])
    metrics.append(cw['lat_90'][i-120])
for i in range(0,30):
#    print metrics[i] , m.predict(np.array([values[i]]))[0]
    print abs(metrics[i]-m.predict(np.array([values[i]]))[0])
#m.plot()
print(m)
