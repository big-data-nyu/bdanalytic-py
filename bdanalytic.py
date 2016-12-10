from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark import HiveContext
from functools import partial
from pprint import pprint
import sys
import math

import subprocess as sp
sp.call('clear',shell=True)

conf = SparkConf().setAppName('bdanalytic').setMaster('local[*]')
sc = SparkContext(conf=conf)

num_clusters = 6

timecat = ['morning', 'afternoon', 'evening', 'latenight']
timeseason = ['spring','summer','fall','winter']

accidentDataFrame = HiveContext(sc).sql('select * from accident')
streetDataFrame = HiveContext(sc).sql('select * from street')

def deg2rad(degrees):
     radians = math.pi * degrees / 180
     return radians

def rad2deg(radians):
     degrees = 180 * radians / math.pi
     return degrees

# streetbroadcast = sc.broadcast(streetDataFrame.rdd.collect())

streetcollection = streetDataFrame.rdd.collect()

def pre_cluster_prepare(row_data):
     split = row_data.split(',')
     v1factor = str(split[16]).strip(",' ")
     v1type = str(split[18]).strip(",' ")
     if v1factor == 'unknown': #remove for FP growth algorithm- needs unique items in transaction
          #v1factor = 'unknownfactor'
          return # we dont want unknowns since it does not generate good rules
     if v1type == 'unknown': #remove for FP growth algorithm- needs unique items in transaction
          #v1type = 'unknowntype'
          return # we dont want unknowns since it does not generate good rules
     #season, time of day , borough, street, probable cause of accident, vehicle type, street rating, injured, killed
     return [str(split[0]).strip(",' "),str(split[1]).strip(",' "),str(split[2]).strip(",' "),str(split[6]).strip(",' "),v1factor,v1type,str(split[20]).strip(",' "),int(split[9]),int(split[10])]

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

# distres = accidentDataFrame.rdd.map(partial(modifyAndJoin,street=streetbroadcast,threshold=0.3))
distres = accidentDataFrame.rdd.map(partial(modifyAndJoin,threshold=0.3))
distres = distres.filter(lambda x: x is not None)
#distres.foreach(print)
#distres.saveAsTextFile('sendtoml')

raw_data = sc.textFile('ml.txt').filter(lambda x: x is not None)

# Group by v1 factor 
# Prints the top 5 factors for accident in NYC
groupv1factor = raw_data.groupBy(lambda word: str(word.split(',')[16])).collect()
grouped_v1_factor = [(group[0],len(group[1])) for group in groupv1factor]
grouped_v1_factor = sorted(grouped_v1_factor,key = lambda x: x[1], reverse = True)
pprint(grouped_v1_factor[1:6]) #removing unknown


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

from kmodes import kprototypes
from kmodes import kmodes
import numpy as np

X= np.array(kmode_input)
kproto = kprototypes.KPrototypes(n_clusters=num_clusters, init='Cao', verbose=2)
# kmodes_cao = kmodes.KModes(n_clusters=4, init='Cao', verbose=1)
# clusters = kmodes_cao.fit(X)
# print(clusters)
# print('k-modes (Cao) centroids:')
# print(kmodes_cao.cluster_centroids_)
# Print training statistics
# print('Final training cost: {}'.format(kmodes_cao.cost_))
# print('Training iterations: {}'.format(kmodes_cao.n_iter_))
#season, time of day , borough, street, probable cause of accident, vehicle type, street rating
clusters = kproto.fit_predict(X, categorical=[0,1,2,3,4,5,6])
# pprint(kproto.cluster_centroids_)
# pprint(kproto.cost_)
# pprint(kproto.n_iter_)
# print(clusters)

# create all files
list_of_files=[]
for i in range(num_clusters):
     file = open('cluster_'+str(i)+'.csv', 'w')
     list_of_files.append(file)

# store all clusters
for idx,element in enumerate(clusters):
     row = kmode_input[idx]
     list_of_files[int(element)].write('  '.join([str(x).replace("'","") for x in row[0:len(row)-2]]) + '\n')

for i in range(num_clusters):
     list_of_files[i].close()
