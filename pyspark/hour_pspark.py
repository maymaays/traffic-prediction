from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def init():
        conf = SparkConf().setAppName('findpats').setMaster('local')
        return SparkContext(conf=conf), SparkSession.builder \
                                                .appName('findpats') \
                                                .getOrCreate()                            
sc, sqlContext = init()

# load file from csv
gps = sqlContext.read.load('./data/raw_data/11 a 20/11-15/gps_20161115',
                          format='com.databricks.spark.csv', 
                          inferSchema='true')
oldrows = gps.schema.names
newrows = ['vehicle_id', 'order_id', 'universal_time', 'longitude', 'latitude']
from functools import reduce
gps = reduce(lambda gps, idx: gps.withColumnRenamed(oldrows[idx], newrows[idx]), range(len(oldrows)), gps)
gps.registerTempTable('gps')

# Create zones
gps_zone1 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.0422 AS DOUBLE) AND longitude <= CAST(104.05967 AS DOUBLE) AND latitude >= CAST(30.7090575 AS DOUBLE) AND latitude <= CAST(30.72774 AS DOUBLE)')
gps_zone1.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone1", sep="|")
gps_zone2 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.05967 AS DOUBLE) AND longitude <= CAST(104.07714 AS DOUBLE) AND latitude >= CAST(30.7090575 AS DOUBLE) AND latitude <= CAST(30.72774 AS DOUBLE)')
gps_zone2.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone2", sep="|")
gps_zone3 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.07714 AS DOUBLE) AND longitude <= CAST(104.09461 AS DOUBLE) AND latitude >= CAST(30.7090575 AS DOUBLE) AND latitude <= CAST(30.72774 AS DOUBLE)')
gps_zone3.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone3", sep="|")

gps_zone4 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.09461 AS DOUBLE) AND longitude <= CAST(104.11208 AS DOUBLE) AND latitude >= CAST(30.7090575 AS DOUBLE) AND latitude <= CAST(30.72774 AS DOUBLE)')
gps_zone4.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone4", sep="|")
gps_zone5 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.11208 AS DOUBLE) AND longitude <= CAST(104.12955 AS DOUBLE) AND latitude >= CAST(30.7090575 AS DOUBLE) AND latitude <= CAST(30.72774 AS DOUBLE)')
gps_zone5.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone5", sep="|")
gps_zone6 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.0422 AS DOUBLE) AND longitude <= CAST(104.05967 AS DOUBLE) AND latitude >= CAST(30.690375 AS DOUBLE) AND latitude <= CAST(30.7090575 AS DOUBLE)')
gps_zone6.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone6", sep="|")

gps_zone7 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.05967 AS DOUBLE) AND longitude <= CAST(104.07714 AS DOUBLE) AND latitude >= CAST(30.690375 AS DOUBLE) AND latitude <= CAST(30.7090575 AS DOUBLE)')
gps_zone7.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone7", sep="|")
gps_zone8 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.07714 AS DOUBLE) AND longitude <= CAST(104.09461 AS DOUBLE) AND latitude >= CAST(30.690375 AS DOUBLE) AND latitude <= CAST(30.7090575 AS DOUBLE)')
gps_zone8.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone8", sep="|")
gps_zone9 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.09461 AS DOUBLE) AND longitude <= CAST(104.11208 AS DOUBLE) AND latitude >= CAST(30.690375 AS DOUBLE) AND latitude <= CAST(30.7090575 AS DOUBLE)')
gps_zone9.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone9", sep="|")

gps_zone10 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.11208 AS DOUBLE) AND longitude <= CAST(104.12955 AS DOUBLE) AND latitude >= CAST(30.690375 AS DOUBLE) AND latitude <= CAST(30.7090575 AS DOUBLE)')
gps_zone10.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone10", sep="|")
gps_zone11 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.0422 AS DOUBLE) AND longitude <= CAST(104.05967 AS DOUBLE) AND latitude >= CAST(30.6716925 AS DOUBLE) AND latitude <= CAST(30.690375 AS DOUBLE)')
gps_zone11.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone11", sep="|")
gps_zone12 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.05967 AS DOUBLE) AND longitude <= CAST(104.07714 AS DOUBLE) AND latitude >= CAST(30.6716925 AS DOUBLE) AND latitude <= CAST(30.690375 AS DOUBLE)')
gps_zone12.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone12", sep="|")

gps_zone13 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.07714 AS DOUBLE) AND longitude <= CAST(104.09461 AS DOUBLE) AND latitude >= CAST(30.6716925 AS DOUBLE) AND latitude <= CAST(30.690375 AS DOUBLE)')
gps_zone13.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone13", sep="|")
gps_zone14 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.09461 AS DOUBLE) AND longitude <= CAST(104.11208 AS DOUBLE) AND latitude >= CAST(30.6716925 AS DOUBLE) AND latitude <= CAST(30.690375 AS DOUBLE)')
gps_zone14.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone14", sep="|")
gps_zone15 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.11208 AS DOUBLE) AND longitude <= CAST(104.12955 AS DOUBLE) AND latitude >= CAST(30.6716925 AS DOUBLE) AND latitude <= CAST(30.690375 AS DOUBLE)')
gps_zone15.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone15", sep="|")

gps_zone16 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.0422 AS DOUBLE) AND longitude <= CAST(104.05967 AS DOUBLE) AND latitude >= CAST(30.65301 AS DOUBLE) AND latitude <= CAST(30.6716925 AS DOUBLE)')
gps_zone16.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone16", sep="|")
gps_zone17 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.05967 AS DOUBLE) AND longitude <= CAST(104.07714 AS DOUBLE) AND latitude >= CAST(30.65301 AS DOUBLE) AND latitude <= CAST(30.6716925 AS DOUBLE)')
gps_zone17.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone17", sep="|")
gps_zone18 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.07714 AS DOUBLE) AND longitude <= CAST(104.09461 AS DOUBLE) AND latitude >= CAST(30.65301 AS DOUBLE) AND latitude <= CAST(30.6716925 AS DOUBLE)')
gps_zone18.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone18", sep="|")

gps_zone19 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.09461 AS DOUBLE) AND longitude <= CAST(104.11208 AS DOUBLE) AND latitude >= CAST(30.65301 AS DOUBLE) AND latitude <= CAST(30.6716925 AS DOUBLE)')
gps_zone19.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone19", sep="|")
gps_zone20 = sqlContext.sql('SELECT * FROM gps WHERE longitude >= CAST(104.11208 AS DOUBLE) AND longitude <= CAST(104.12955 AS DOUBLE) AND latitude >= CAST(30.65301 AS DOUBLE) AND latitude <= CAST(30.6716925 AS DOUBLE)')
gps_zone20.repartition(1).write.csv("./zone/day15_zone1_20/gps_zone20", sep="|")
