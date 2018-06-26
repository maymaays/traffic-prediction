from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from functools import reduce

def init():
        conf = SparkConf().setAppName('findpats').setMaster('local')
        return SparkContext(conf=conf), SparkSession.builder \
                                                .appName('findpats') \
                                                .getOrCreate()

def columnrename(db, newrows):
        oldrows = db.schema.names
        return reduce(lambda db, idx: db.withColumnRenamed(oldrows[idx], newrows[idx]), range(len(oldrows)), db)       

sc, sqlContext = init()

def read_file(path):
        return sqlContext.read.load(path, 
                          format='com.databricks.spark.csv', 
                          inferSchema='true')

def show_table(query):
        return sqlContext.sql(query).show()

# load file from data
# order = read_file('./data/11-01/order_20161101')

order = sqlContext.read.load('./data/raw_data/02a10/11-03/order_20161103', 
                          format='com.databricks.spark.csv', 
                          inferSchema='true')

order.count() # 209423

# show schema of data
order.printSchema() 

# oldrows = order.schema.names
# newrows =  ['order_id', 'departure_time', 
#         'arrival_time', 'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati']
# order = reduce(lambda order, idx: order.withColumnRenamed(oldrows[idx], newrows[idx]), range(len(oldrows)), order)       

order = columnrename(order, ['order_id', 'departure_time', 
        'arrival_time', 'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati'])

order.printSchema() 

gps = read_file('./data/11-01/gps_20161101')

gps = sqlContext.read.load('./data/raw_data/02a10/11-03/gps_20161103', 
                          format='com.databricks.spark.csv', 
                          inferSchema='true')
# oldrows = gps.schema.names
# newrows = ['vehicle_id', 'order_id', 
#                 'universal_time', 'longitude', 'latitude']
# gps = reduce(lambda gps, idx: gps.withColumnRenamed(
#                 oldrows[idx], newrows[idx]), range(len(oldrows)), gps)   


gps = columnrename(gps, ['vehicle_id', 'order_id', 
                'universal_time', 'longitude', 'latitude'])

gps.printSchema()

order.registerTempTable('order')
gps.registerTempTable('gps')

order_id = show_table('SELECT order_id, COUNT(*) FROM order GROUP BY order_id')

# order_id = sqlContext.sql('SELECT order_id, COUNT(*) FROM order GROUP BY order_id').show()

# join table 
order_join = order.join(gps, order.order_id == gps.order_id)

order_join.printSchema()
order_join.registerTempTable('order_join')

order_join = show_table("SELECT order.order_id, order.departure_time, order.arrival_time, gps.universal_time FROM order LEFT JOIN gps ON order.order_id = gps.order_id")

# order_join = sqlContext.sql("SELECT order.order_id, order.departure_time, order.arrival_time, gps.universal_time FROM order LEFT JOIN gps ON order.order_id = gps.order_id")
order_join.registerTempTable('order_join')


# select the minimum of universal time for each id
# order_select = sqlContext.sql("SELECT order_id, departure_time, min(universal_time) AS min_universal_time FROM order_join GROUP BY order_id, departure_time")
# order_select.registerTempTable('order_select')
# first solution
# order_count = sqlContext.sql("SELECT COUNT(*) FROM order_select WHERE min_universal_time - departure_time > 60")

# 78896 
# 209423

# order_select = sqlContext.sql("SELECT order_id, (CAST(departure_time AS bigint) + CAST(arrival_time AS bigint)) / 2 AS expected_avg, AVG(universal_time) AS actual_avg FROM order_join GROUPBY order_id, expected_avg")
# A-, B
# order_count = sqlContext.sql("SELECT COUNT(*) FROM order_select WHERE expected_avg-actual_avg > 180").show()
# A,B-
# order_count = sqlContext.sql("SELECT COUNT(*) FROM order_select WHERE actual_avg - expected_avg > 180").show()
# normal
# order_count = sqlContext.sql("SELECT COUNT(*) FROM order_select WHERE ABS(actual_avg - expected_avg) > 180").show()

# third solution

# select order_id, departure, arrival, min, max(universal_time)
order_select = sqlContext.sql("SELECT order_id, departure_time, arrival_time, min(universal_time) AS min_universal_time, max(universal_time) AS max_universal_time FROM order_join GROUP BY order_id, departure_time, arrival_time")
order_select.registerTempTable('order_select')

# first pattern (normal)
order_normal = sqlContext.sql("SELECT order_id FROM order_select WHERE min_universal_time - departure_time <= 300 AND arrival_time - max_universal_time <= 300")
order_normal.registerTempTable('order_normal')

# seceond pattern (A-, B)
order_aminus = sqlContext.sql("SELECT order_id FROM order_select WHERE min_universal_time - departure_time > 300 AND arrival_time - max_universal_time <= 300")
order_aminus.registerTempTable('order_aminus')
# third pattern (A, B-)
order_bminus = sqlContext.sql("SELECT order_id FROM order_select WHERE min_universal_time - departure_time <= 300 AND arrival_time - max_universal_time > 300")
order_bminus.registerTempTable('order_bminus')

# fourth pattern (weak signal)
order_weak = sqlContext.sql("SELECT order_id FROM order_select WHERE min_universal_time - departure_time > 300 AND arrival_time - max_universal_time > 300")
order_weak.registerTempTable('order_weak')

# show order id of normal pattern
order_normal.show()

# normal = 64471, 35.56%
order_normal.count()

# (A-, B) = 52777, 29.13%
order_aminus.count()

# (A, B-) = 52292, 28.86% 
order_bminus.count()

# weak = 11632, 6.42%
order_weak.count()

# total = 181172



order_normal.repartition(1).write.csv("normal_test_id", sep="|")

order_aminus.repartition(1).write.csv("aminus_id", sep="|")

order_bminus.repartition(1).write.csv("bminus_id", sep="|")

order_weak.repartition(1).write.csv("weak_id", sep="|")


# day 2: 67171, 54155, 53540, 11395, total = 186261