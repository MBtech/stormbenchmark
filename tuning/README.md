# Automatic Parameter Tuning
This folder contains the scripts and examples for running automatic parameter tuning. 

### Note: This section is going through major overhaul and refractoring for ease of use. In the meantime following guide can be used to test the tuning framework

## Setup:
Clone this repository onto the master node of your cluster

The framework has several dependencies and that can be installed using setup.sh script
```./setup.sh```

Build the benchmark
    
    mvn package -DskipTests

Download the TDigestService

    git clone https://github.com/MBtech/TDigestService.git

Build TDigestServer and Install the library using 

    mvn package
    mvn clean install

TDigest is used for collecting latency metrics from the benchmark topologies. However, if you plan on using your own topologies and metrics then there is no need for this step. 

Setup and start your Apache Storm cluster. Take a look at the guide on [Storm's website](http://storm.apache.org/releases/1.0.3/Setting-up-a-Storm-cluster.html)

Setup and start HDFS cluster. Check online documentation on how to setup HDFS Cluster (I will soon add the ansible playbooks that I use to start and setup Apache Storm and HDFS Clusters)

Copy the files in the stormbenchmark/data folder into the root directory of the HDFS cluster. If you store the files somewhere else you would have to change the path in the topology conf files accordingly. 


(WIP)


## Usage:
The file ```tuning/environment``` needs to be change and relevant paths need to be replaced for the system to work properly

The optimization algorithms currently available are:
- Hill Climbing
- Hill Climbing with heuristic sampling
- Rule based algorithm
- Genetic algorithm

### Hill Climbing
     
     python hc.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml
     
### Hill Climbing with heuristic sampling
     
     python mhc.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml
     
### Rule-based Algorithm
     
     python rule_based.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml yamlfiles/lat_rank_rc.yaml yamlfiles/tp_rank_rc.yaml
     
### Genetic Algorithm
     
     python ga.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml

This will run the rolling top words topology with the goal of decreasing 90th percentile latency while maintaining a minium throughput of 150k tuples per second using the specified algorithm. By default the optimization budget is 50 iterations and 12 initial samples will be generated. The rule based algorithm terminates after it's optimization phases are done instead of running for 50 iterations

The topology specified in the arguments to these scripts should correspond to the topology name set in the `environment` file
