# Storm's Automatic Parameter tuning [![Build Status](https://travis-ci.org/intel-hadoop/storm-benchmark.svg?branch=master)](https://travis-ci.org/intel-hadoop/storm-benchmark?branch=master)

### Note: The repository is going through major overhaul and refractoring to make it ease to use. Further documentation will be added along with refractoring process. Current version of the system can be used by following the guide in `tuning/README.md`. The update has been delayed because of my other engagements if you are having any problems in using it. Please contact me and I might be able to solve your issue directly. 

## Benchmark
The benchmarking framework is main based on the [Intel storm benchmark](https://github.com/intel-hadoop/storm-benchmark). We have added several other benchmarking applications as well.

## Automatic Parameter Tuning
The automatic parameter tuning for the example topologies can be experimented with using the guide available in `tuning/README.md`.
There is some ongoing work regarding addition of new black box algorithms for optimization. More information on the new algorithms will be added as they are tested and pushed into the repo. 

# Automatic Parameter Tuning
This folder contains the scripts and examples for running automatic parameter tuning. 

### Note: This section is going through major overhaul and refractoring for ease of use. In the meantime following guide can be used to test the tuning framework

## Setup:
If you want to run the benchmark portion of this repository, use the following guide for setup and usage:

- Clone this repository onto the master node of your cluster using ```git clone https://github.com/MBtech/stormbenchmark.git```

- The framework has several dependencies and that can be installed using setup.sh script
```./setup.sh```

- Build the benchmark applications
    
    mvn package -DskipTests

- Download the TDigestService

    git clone https://github.com/MBtech/TDigestService.git

- Build TDigestServer and Install the library using 

    mvn package
    mvn clean install

TDigest is used for collecting latency metrics from the benchmark topologies. However, if you plan on using your own topologies and metrics then there is no need for this step. We are working towards providing support for custome metrics. 

- Setup and start your Apache Storm cluster. Take a look at the guide on [Storm's website](http://storm.apache.org/releases/1.0.3/Setting-up-a-Storm-cluster.html)

- Setup and start HDFS cluster. Check online documentation on how to setup HDFS Cluster (I will soon add the ansible playbooks that I use to start and setup Apache Storm and HDFS Clusters)

- Copy the files in the stormbenchmark/data folder into the root directory of the HDFS cluster. If you store the files somewhere else you would have to change the path in the topology conf files accordingly. 

- On the master node, copy topologyConfs folder to ~/.storm that is used as a default folder where configuration files are looked up.
    mkdir ~/.storm
    cp -R topologyConfs/* ~/.storm/

- In your ~/.storm conf files replace the ```topology.tdigestserver``` parameter value with the IP address of the host where TDigest server will be runing and replace the value or ```topology.tdigestserver.port``` with the respective port. By default the example usage script starts the t-digest server in same host where you execute the script and the t-digest port can be found/configured using ```tuning/environment``` file.
 
## Usage
Run example_usage.sh in the ```tuning``` folder to run benchmark and get latency and throughput numbers in a file named ```numbers.csv```. The example_usage.sh file will run the rollingcount benchmark. Benchmark and configuration selection can be performed by editing the ```tuning/environment``` file. 

Open and read the example.sh script to see the steps required to start the benchmark and collect metrics

## Contact
For any comments or question please contact me:
muhammad.bilal@uclouvain.be
