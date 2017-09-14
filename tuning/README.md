# Automatic Parameter Tuning
This folder contains the scripts and examples for running automatic parameter tuning. 

### Note: This section is going through major overhaul and refractoring for ease of use. In the meantime following guide can be used to test the tuning framework

## Setup:
The framework has several dependencies and that can be installed using setup.sh script
```./setup.sh```

Build the benchmark
```mvn package -DskipTests```

Download the TDigestService
```git clone https://github.com/MBtech/TDigestService.git```
TDigest is used for collecting latency metrics from the benchmark topologies. However, if you plan on using your own topologies and metrics then there is no need for this step. 

(WIP)


## Usage:
The file ```tuning/environment``` needs to be change and relevant paths need to be replaced for the system to work properly

The optimizer python script links to the three algorithms that have been added to the framework. As a example you can run the framework using the following command

```python optimizer.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml yamlfiles/lat_rank_rc.yaml yamlfiles/tp_rank_rc.yaml hc```

This will run the rolling top words topology with the goal of decreasing 90th percentile latency while maintaining a minium throughput of 150k tuples per second using the hill climbing algorithm. By default the optimization budget is 50 iterations and 12 initial samples will be generated. 

Available algorithms are ```hc, mhc, ga, rule_based``` at the moment
