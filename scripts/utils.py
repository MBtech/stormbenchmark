import subprocess
from randomize import selective_write
import pandas
import sys

# Write the configuration to the file
# Can write multiple entries based on the start end end entries
def write(design_space, start,end ,basefile):
    folder = "config_files"
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in range(start,end):
        fname = folder+"/"+"test"+str(i)+".yaml"
        selective_write(design_space[i],fname, basefile)

# Get results for a particular configuration. returns max int if the configuration failed to run
def get_results(start, end, design_space, basefile, metric):
    write(design_space, start,end ,basefile)
    for i in range(start, end):
        bashCommand = "./onescript.sh " + str(i)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]

    cw = pandas.read_csv("numbers.csv")
    metric_values = list()
    ret_array = list()
    for i in range(start, end):
        if i in list(cw['no.']):
            ret_array.append(i)
            metric_values.append(cw[metric][list(cw['no.']).index(i)])
        else:
           ret_array.append(i)
           metric_values.append(sys.maxint)

    print metric_values
    print ret_array
    return metric_values,ret_array
