from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from functools import reduce

def init():
        conf = SparkConf().setAppName('findpats').setMaster('local')
        return SparkContext(conf=conf), SparkSession.builder \
                                                .appName('findpats') \
                                                .getOrCreate()                            
sc, sqlContext = init()

# load file from csv
gps = sqlContext.read.load('./data/gps_hour_data/22/gps_22_day1/part-00000-36e8eed7-800e-447a-b58d-d022803e328a-c000.csv', 
                          format='com.databricks.spark.csv', 
                          inferSchema='true', sep='|')


oldrows = gps.schema.names
newrows = ['vehicle_id', 'order_id', 
                'universal_time', 'longitude', 'latitude']
gps = reduce(lambda gps, idx: gps.withColumnRenamed(
                oldrows[idx], newrows[idx]), range(len(oldrows)), gps)   

gps.printSchema()

gps.registerTempTable('gps')

# query 
gps_1 = sqlContext.sql("SELECT * FROM gps WHERE universal_time = 1")
gps_1.registerTempTable('gps_1')

# write data into csv file
gps_1.repartition(1).write.csv("gps_1_day3", sep="|")
