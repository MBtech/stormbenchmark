# Automatic Parameter Tuning
This folder contains the scripts and examples for running automatic parameter tuning. 

### Note: This section is going through major overhaul and refractoring for ease of use. In the meantime following guide can be used to test the tuning framework

## Setup:
The framework has several dependencies and that can be installed using setup.sh script
```./setup.sh```

## Usage:
The optimizer python script links to the three algorithms that have been added to the framework. As a example you can run the framework using the following command

```python optimizer.py yamlfiles/conf_rollingtopwords_hc.yaml rollingtopwords.yaml lat_90,throughput=150000 yamlfiles/relations.yaml yamlfiles/lat_rank_rc.yaml yamlfiles/tp_rank_rc.yaml hc```

This will run the rolling top words topology with the goal of decreasing 90th percentile latency while maintaining a minium throughput of 150k tuples per second using the hill climbing algorithm. By default the optimization budget is 50 iterations and 12 initial samples will be generated. 
