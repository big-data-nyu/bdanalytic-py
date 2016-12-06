from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark import HiveContext
from functools import partial
import sys
import math

conf = SparkConf().setAppName('bdanalytic').setMaster('local[*]')
sc = SparkContext(conf=conf)

# boroughcat = ['manhattan','brooklyn','bronx','queens','staten island']
timecat = ['morning', 'afternoon', 'evening', 'latenight']
timeseason = ['spring','summer','fall','winter']
# v1typecat = ['passenger vehicle,motorcycle,van,other,unknown,bus,taxi,bicycle,pick-up truck,livery vehicle,ambulance,fire truck,scooter,pedicab']
# v1factorcat = ['driver inattention/distraction','failure to yield right-of-way','fatigued/drowsy','turning improperly','driver inexperience','pavement slippery','alcohol involvement','view obstructed/limited','aggressive driving/road rage','brakes defective','obstruction/debris','pavement defective','lane marking improper/inadequate']

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
     #print(split[1],split[2],split[6],split[8],split[9], split[20])
     return(split[1],split[2],split[6],split[8],split[9], split[16],split[20])

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

mldata = sc.textFile('ml.txt').filter(lambda x: x is not None).map(lambda x: pre_cluster_prepare(x))

groupv1type = mldata.groupBy(lambda word: str(word[5]))
print([(groupv1type[0],[i for i in groupv1type[1]]) for groupv1type in mldata.collect()])


#ML CLUSTERING START
#load the text file that contains the join of two datasets

ml_prepared = map(lambda x: pre_cluster_prepare(x))
kmode_input = ml_prepared.collect();

from kmodes import kprototypes
import numpy as np

X= np.array(kmode_input)
kproto = kprototypes.KPrototypes(n_clusters=4, init='Cao', verbose=2)
clusters = kproto.fit_predict(X, categorical=[0,1,2,5,6])
print(kproto.cluster_centroids_)
print(kproto.cost_)
print(kproto.n_iter_)
