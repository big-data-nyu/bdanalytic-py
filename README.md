#Analysis of road accidents in NYC and their contributing factors.
#Big Data Fall 2016

The analytic is available in
https://github.com/big-data-nyu/bdanalytic-py

##Below is the folder structure of our project.

![Alt text](tree.png?raw=true "Tree")

### src/bd_analytic.py 
This python file loads the data from Hive, performs a Map based on location and runs the K-mode clustering algorithm.
### src/bd_rule_mining.scala 
This scala file loads the individual clusters from HDFS and runs FPGrowth and Association Rule mining MLlib algorithms to identify Frequently appearing patters and rules.


##	Please ensure python 2.7 is installed since K-mode clustering is dependent .

##	Load the data into Hive using the commands from dataset/hive_load_commands.txt 

$	export PYSPARK_PYTHON =/usr/bin/python2.7
$	pip install --upgrade kmodes

$	pyspark bd_analytic.py

## 	Once the python script runs, the cluster_*.csv files have to be added into HDFS root directory.
$	spark-shell -i bd_rule_mining.scala
