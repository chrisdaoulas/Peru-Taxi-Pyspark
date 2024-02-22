# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import struct
from pyspark.sql.functions import col


os.chdir('C:\\Users\\cdaou\\OneDrive\\Documents\\MSBDGA\\INFO-600\\PROJECT\\INFO-600_Project\\')

os.environ['PYSPARK_SUBMIT_ARGS'] ="--conf spark.driver.memory=3g  pyspark-shell"
os.environ['JAVA_HOME'] ="C:\Program Files\Java\jre1.8.0_311"
os.environ['SPARK_HOME'] ="C:\\Users\\cdaou\\anaconda3\\envs\\python\\Lib\\site-packages\\pyspark"

from pyspark.sql import SparkSession

try: 
    spark
    print("Spark application already started. Terminating existing application and starting new one")
    spark.stop()
except: 
    pass

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("demoRDD") \
    .getOrCreate()
    

sc=spark.sparkContext

sc._conf.getAll()

#Loading DataFrames
drivers = spark.read.csv('drivers.csv', header=True, inferSchema=True)

zones = spark.read.json("zones.json",multiLine=True)

#Drivers Transformation for coordinates
driverscomp = drivers.withColumn("coordinates",F.struct(drivers.latitude,drivers.longitude))
driverscomp= sc.parallelize(driverscomp.collect()).toDF()
driverzone = driverscomp.select("driver","coordinates").collect()
driverzonedf = sc.parallelize(driverzone).toDF()

#Zones Transformation for polygon coordinates and zones
zonesfull = zones.select("zones").collect()[0]['zones']
zonesfull = sc.parallelize(zonesfull).toDF()


polycoorslist = zonesfull.select("polygon").collect()

id_zoneslist = zonesfull.select("id_zone").collect()
id_zones= sc.parallelize(id_zoneslist).toDF()




    
##define function to check if point in polygon using the shapely package
#It will run a check to see if each driver location point is in
#every of the 51 polygons list and then return the equivalent zone if it is in the list
#or the value "Zone not found" if it is not
#to do:
#    can even graph the world and put the polygons inside



def inpolygoncheck(coordinate):
    coordinate = Point(coordinate) # turn point into readable format
    i=0
    position=0
    for poly in polycoorslist:
        polygon = Polygon(poly[0])
        check1 = polygon.contains(coordinate)# checks if driver's position is     in polygon
        check2 = polygon.touches(coordinate)# check if driver's position lies on border of polygon 
        if check1==True or check2==True:
            position = id_zoneslist[i][0]
            return position
        i+=1        
    position=None
    return position

udfzone = F.udf(lambda x: inpolygoncheck(x),StringType())


driverswithzone = driverzonedf.withColumn("Zone",udfzone("coordinates")).drop("coordinates")
driverswithzone=sc.parallelize(driverswithzone).toDF()


#duplicate rows
driverswithzones = driverscomp.join(driverswithzone, on="driver", how="inner").dropDuplicates()



