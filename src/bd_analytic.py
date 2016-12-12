from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark import HiveContext
from functools import partial
from pprint import pprint
from kmodes import kprototypes
from kmodes import kmodes
import numpy as np
import sys
import math

#NOTE : This Py file has to be run with in python 2.7 environment

#clear the shell
import subprocess as sp
sp.call('clear',shell=True)

#create spark configuration and spark context
conf = SparkConf().setAppName('bdanalytic').setMaster('local[*]')
sc = SparkContext(conf=conf)

num_clusters = 6

timecat = ['morning', 'afternoon', 'evening', 'latenight']
timeseason = ['spring','summer','fall','winter']

#load the data frames from accident and street hive database
accidentDataFrame = HiveContext(sc).sql('select * from accident')
streetDataFrame = HiveContext(sc).sql('select * from street')

#function converts from degree to radians
def deg2rad(degrees):
     radians = math.pi * degrees / 180
     return radians

#function converts from radians to degree
def rad2deg(radians):
     degrees = 180 * radians / math.pi
     return degrees

streetcollection = streetDataFrame.rdd.collect()

#create new tuple for the K-Mode clustering Algorithm
def pre_cluster_prepare(row_data):
     split = row_data.split(',')
     #season, time of day , borough, street, probable cause of accident, vehicle type, street rating, injured, killed
     return [str(split[0]).strip(",' "),str(split[1]).strip(",' "),str(split[2]).strip(",' "),str(split[6]).strip(",' "),str(split[16]).strip(",' "),str(split[18]).strip(",' "),str(split[20]).strip(",' "),int(split[9]),int(split[10])]

#location matching algorithm
#tries to map a street data tuple to an accident data tuple with in a threshold radius
def modifyAndJoin(accidentRow,threshold):
     #Season Comparison Code
     accidentdate = accidentRow[0]
     comparedate = int(accidentdate[:accidentdate.find('/')])
     if comparedate>=3 and comparedate<=5:
          accidentdate = timeseason[0]
     elif comparedate>=6 and comparedate<=8:
          accidentdate = timeseason[1]
     elif comparedate>=9 and comparedate<=11:
          accidentdate = timeseason[2]
     elif comparedate==12 or comparedate<=2:
          accidentdate = timeseason[3]
     #Time Comparison Code
     accidenttime = accidentRow[1]
     comparetime = int(accidenttime[:accidenttime.find(':')])
     if comparetime>=4 and comparetime<=11:
          accidenttime = timecat[0]
     if comparetime>11 and comparetime<=15:
          accidenttime = timecat[1]
     if comparetime>15 and comparetime<=21:
          accidenttime = timecat[2]
     if comparetime>21 or comparetime<=4:
          accidenttime = timecat[3]
     #Location Comparison
     latacdnt = float(accidentRow[4])
     lonacdnt = float(accidentRow[5])
     d1min = sys.maxint
     d2min = sys.maxint
     drow = None
     phi1 = deg2rad(latacdnt)
     for eachrow in streetcollection:
          startx = float(eachrow[0])
          starty = float(eachrow[1])
          phi2 = deg2rad(startx)
          endx = float(eachrow[2])
          endy = float(eachrow[3])
          R = 6371
          dLat = deg2rad(startx-latacdnt)
          dLon = deg2rad(starty-lonacdnt) 
          a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(phi1) * math.cos(phi2) * math.sin(dLon/2) * math.sin(dLon/2) 
          c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)) 
          d1 = R * c
          phi2 = deg2rad(endx)
          dLat = deg2rad(endx-latacdnt)
          dLon = deg2rad(endy-lonacdnt) 
          a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(phi1) * math.cos(phi2) * math.sin(dLon/2) * math.sin(dLon/2) 
          c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)) 
          d2 = R * c
          if d1<d1min:
               d1min = d1
               drow = (accidentdate,accidenttime,str(accidentRow[2]),str(accidentRow[3]),accidentRow[4],accidentRow[5],str(accidentRow[6]),str(accidentRow[7]),accidentRow[8],accidentRow[9],accidentRow[10],accidentRow[11],accidentRow[12],accidentRow[13],accidentRow[14],accidentRow[15],str(accidentRow[16]),str(accidentRow[17]),str(accidentRow[18]),str(accidentRow[19]),str(eachrow[9]))
          if d2<d2min:
               d2min = d2               
               drow = (accidentdate,accidenttime,str(accidentRow[2]),str(accidentRow[3]),accidentRow[4],accidentRow[5],str(accidentRow[6]),str(accidentRow[7]),accidentRow[8],accidentRow[9],accidentRow[10],accidentRow[11],accidentRow[12],accidentRow[13],accidentRow[14],accidentRow[15],str(accidentRow[16]),str(accidentRow[17]),str(accidentRow[18]),str(accidentRow[19]),str(eachrow[9]))
     if d1min<=threshold or d2min<=threshold:
          return drow 
     else: return None

distres = accidentDataFrame.rdd.map(partial(modifyAndJoin,threshold=0.3))
distres = distres.filter(lambda x: x is not None)
#distres.foreach(print) #This has to be uncommented one time to obtain the Mapped data set
#distres.saveAsTextFile('sendtoml') #This has to be uncommented one time to obtain the Mapped data set

#Load the ml.txt file
raw_data = sc.textFile('ml.txt').filter(lambda x: x is not None)

# Group by v1 factor 
# Prints the top 5 factors for accident in NYC
groupv1factor = raw_data.groupBy(lambda word: str(word.split(',')[16])).collect()
grouped_v1_factor = [(group[0],len(group[1])) for group in groupv1factor]
grouped_v1_factor = sorted(grouped_v1_factor,key = lambda x: x[1], reverse = True)
pprint(grouped_v1_factor[0:5]) #removing unknown


# Group by v1 factor
# Prints the top 5 types of vehicles involved  in an accident in NYC
groupv1type = raw_data.groupBy(lambda word: str(word.split(',')[18])).collect()
grouped_v1_types = [(group[0],len(group[1])) for group in groupv1type]
grouped_v1_types = sorted(grouped_v1_types,key = lambda x: x[1], reverse = True)
pprint(grouped_v1_types[0:5]) 


#ML CLUSTERING START
#load the text file that contains the join of two datasets

ml_prepared = raw_data.map(lambda x: pre_cluster_prepare(x))
ml_prepared = ml_prepared.filter(lambda x: x is not None) # added since none rows can be returned
kmode_input = ml_prepared.collect()

#create a Numpy array since kmode requires a numpy array object
X= np.array(kmode_input)
kproto = kprototypes.KPrototypes(n_clusters=num_clusters, init='Cao', verbose=2)
#run K-Mode clustering based on categorical and Numerical attributes in kmode_input
clusters = kproto.fit_predict(X, categorical=[0,1,2,3,4,5,6])

# create all cluster files
list_of_files=[]
for i in range(num_clusters):
     file = open('cluster_'+str(i)+'.csv', 'w')
     list_of_files.append(file)

# store all cluster information into the files
for idx,element in enumerate(clusters):
     row = kmode_input[idx]
     list_of_files[int(element)].write('  '.join([str(x).replace("'","") for x in row[0:len(row)-2]]) + '\n')

#close all file handles
for i in range(num_clusters):
     list_of_files[i].close()
