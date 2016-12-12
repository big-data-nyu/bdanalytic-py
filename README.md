#Analysis of road accidents in NYC and their contributing factors.
#Big Data Fall 2016


##Below is the folder structure of our project.

![Alt text](tree.png?raw=true "Tree")

├── README.md
├── dataset			#This is the input data set.
│   ├── Accident_Data_ToHadoop.csv
│   ├── Street_Data_ToHadoop.csv
│   └── hive_load_commands.txt
├── output			#These are results from the K-mode clustering and Association Rule mining Algorithm.
│   ├── cluster_0.csv
│   ├── cluster_1.csv
│   ├── cluster_2.csv
│   ├── cluster_3.csv
│   ├── cluster_4.csv
│   ├── cluster_5.csv
│   ├── rules_cluster_0.txt
│   ├── rules_cluster_1.txt
│   ├── rules_cluster_2.txt
│   ├── rules_cluster_3.txt
│   ├── rules_cluster_4.txt
│   └── rules_cluster_5.txt
├── pbda_fall16.pptx
├── profiling 		#This folder contains the code and ouput of profiling each data source in Map Reduce.
│   ├── Accident
│   │   ├── Profiling_DS_Accident.java
│   │   ├── Profiling_DS_Accident_Mapper.java
│   │   ├── Profiling_DS_Accident_Reducer.java
│   │   ├── part-r-00000.txt
│   │   └── profilingDS2.jar
│   └── Street
│       ├── Profiling_DS_Street.java
│       ├── Profiling_DS_Street_Mapper.java
│       ├── Profiling_DS_Street_Reducer.java
│       ├── part-r-00000.txt
│       └── profilingDS1.jar
└── src 			#This folder contains our analytic code.
    ├── bd_analytic.py
    └── bd_rule_mining.scala


src/bd_analytic.py loads the data from Hive, performs a Map based on location and runs the K-mode 
clustering algorithm.
src/bd_rule_mining.scala loads the individual clusters from HDFS and runs FPGrowth and Association
Rule mining MLlib algorithms to identify Frequently appearing patters and rules.


##	Please ensure python 2.7 is installed since K-mode clustering is dependent .

##	Load the data into Hive using the commands from dataset/hive_load_commands.txt 

$	export PYSPARK_PYTHON =/usr/bin/python2.7
$	pip install --upgrade kmodes

$	pyspark bd_analytic.py

## 	Once the python script runs, the cluster_*.csv files have to be added into HDFS root directory.
$	spark-shell -i bd_rule_mining.scala