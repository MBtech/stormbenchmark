import subprocess
import re

def get():
    bashCommand = "ansible all -m setup -a 'filter=ansible_processor_*' -i hosts"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = subprocess.check_output(('grep', 'ansible_processor_vcpus'), stdin=process.stdout)
    stdout, stderr = process.communicate()
    if stderr!="":
        print "There was an error"
    array =  output.split("\n")
    total_cores = 0
    for e in array:
        if e!="":
            total_cores +=int(e.strip().split(":")[1])
    return total_cores
