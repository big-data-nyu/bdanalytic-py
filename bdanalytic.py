from pyspark import SparkConf, SparkContext
from pyspark import HiveContext
from __future__ import print_function


boroughcat = ['manhattan','brooklyn','bronx','queens','staten island']
timecat = ['morning', 'afternoon', 'evening', 'latenight']
#Can use for v2 as well
v1typecat = ['passenger vehicle,motorcycle,van,other,unknown,bus,taxi,bicycle,pick-up truck,livery vehicle,ambulance,fire truck,scooter,pedicab']
v1factorcat = ['driver inattention/distraction','failure to yield right-of-way','fatigued/drowsy','turning improperly','driver inexperience','pavement slippery','alcohol involvement','view obstructed/limited','aggressive driving/road rage','brakes defective','obstruction/debris','pavement defective','lane marking improper/inadequate']

accidentDataFrame = HiveContext(sc).sql('select * from accident')
streetDataFrame = HiveContext(sc).sql('select * from street')


def processStreetRow(streetDataFrame):
     startx = streetDataFrame['startx']
     starty = streetDataFrame['starty']
     endx = streetDataFrame['endx']
     endy = streetDataFrame['endy']
     length = streetDataFrame['length']
     #segmentid = streetDataFrame['segmentid']
     width = streetDataFrame['width']
     #usageclass = streetDataFrame['usageclass']
     #rating = streetDataFrame['rating']
     wordrating = streetDataFrame['wordrating']
     #isvalid = streetDataFrame['isvalid']
     return (startx,starty,endx,endy,length,width,wordrating)
     

def processAccidentRow(accidentDataFrame):
     accidentdate = accidentDataFrame['accidentdate']
     accidenttime = accidentDataFrame['accidenttime']
     borough = accidentDataFrame['borough']
     zipcode = accidentDataFrame['zipcode']
     latitude = accidentDataFrame['latitude']
     longitude = accidentDataFrame['longitude']
     onstreet = accidentDataFrame['onstreet']
     crossstreet = accidentDataFrame['crossstreet']
     personsinjured = accidentDataFrame['personsinjured']
     personskilled = accidentDataFrame['personskilled']
     pedestriansinjured = accidentDataFrame['pedestriansinjured']
     pedestrianskilled = accidentDataFrame['pedestrianskilled']
     cyclistsinjured = accidentDataFrame['cyclistsinjured']
     cyclistskilled = accidentDataFrame['cyclistskilled']
     motoristsinjured = accidentDataFrame['motoristsinjured']
     motoristskilled = accidentDataFrame['motoristskilled']
     v1factor = accidentDataFrame['v1factor']
     v2factor = accidentDataFrame['v2factor']
     v1type = accidentDataFrame['v1type']
     v2type = accidentDataFrame['v2type']
     comparetime = int(accidenttime[:accidenttime.find(':')])
     if comparetime>=4 and comparetime<=11:
          accidenttime = timecat[0]
     if comparetime>11 and comparetime<=15:
          accidenttime = timecat[1]
     if comparetime>15 and comparetime<=21:
          accidenttime = timecat[2]
     if comparetime>21 or comparetime<=4:
          accidenttime = timecat[3]
     return (accidentdate,accidenttime,borough,zipcode,latitude,longitude,onstreet,crossstreet,personsinjured,personskilled,pedestriansinjured,pedestrianskilled,cyclistsinjured,cyclistskilled,motoristsinjured,motoristskilled,v1factor,v2factor,v1type,v2type)



#a = []
#accidentDataFrame.foreach(processAccidentRow(x))
#streetDataFrame.foreach(lambda x: processStreetRow(x))
#streetDataFrame.foreach(processStreetRow(x))



accidentdatardd = accidentDataFrame.rdd.map(processAccidentRow)
streetdatardd = streetDataFrame.rdd.map(processStreetRow)
streetbroadcast = sc.broadcast(streetdatardd.collect())
#combinedrdd = accidentdatardd.cartesian(streetdatardd)



# update accident set accidenttime = 
# (case
# when cast(substring_index(accidenttime, ":", 1) as int) >=4 and  cast(substring_index(accidenttime, ":", 1) as int)<=11 THEN 'morning'
# when cast(substring_index(accidenttime, ":", 1) as int) >11 and  cast(substring_index(accidenttime, ":", 1) as int)<=15 THEN 'afternoon'
# when cast(substring_index(accidenttime, ":", 1) as int) >15 and  cast(substring_index(accidenttime, ":", 1) as int)<=21 THEN 'evening'
# when cast(substring_index(accidenttime, ":", 1) as int) >21 or  cast(substring_index(accidenttime, ":", 1) as int)<=4 THEN 'latenight'
# else accidenttime
# end
# )