import os
import pandas as pd
from datetime import datetime
from gmplot import gmplot

# read order file
def read_file(file, columns):
    return pd.read_csv(file, names = columns)

# convert second to timestamp
def second_to_timestamp(df, format):
    return df.apply(lambda t: datetime.fromtimestamp(t).strftime(format))

# order = read_file('./data/11-01/order_20161101', 
#                 ['order_id', 'departure_time', 'arrival_time', 
#                 'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati'])

# order['departure_time'] = second_to_timestamp(order['departure_time'], '%I:%M:%S')
# order['arrival_time'] = second_to_timestamp(order['arrival_time'], '%I:%M:%S')

gps = read_file('./data/raw_data/02a10/11-07/order_20161107',
                ['vehicle_id', 'order_id', 'universal_time', 'longitude', 'latitude'])
gps['universal_time'] = second_to_timestamp(gps['universal_time'], '%H')

gps.to_csv('./gps_hour/gps_hour_day7.csv', header=None, index=False)

# gps = read_file('./gps_hour/gps_hour_day2.csv', ['vehicle_id', 'order_id', 'universal_time', 'longitude', 'latitude'])
# print(gps)

# count (A-, B)
# count = 0
# for _, order_row in order.head(1).iterrows():
#     departure_time = order_row['departure_time']
#     _, first_gps_signal_time = gps.iloc[gps['order_id'].searchsorted(order_row['order_id'], side='left')]['universal_time']
#     print(first_gps_signal_time)
#     if first_gps_signal_time-departure_time>60:
#         count+=1
# print(count)

# # order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))

# # read gps file
# gps = read_file('./data/02a10/11-02/gps_20161102',
#                     names = ["vehicle_id", "order_id", "universal_time", 
#                     "longitude", "latitude"])
# # gps["universal_time"] = gps["universal_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# # print(gps.head(5))
# # for vehicle_id in gps['vehicle_id']:

# # for _, row in gps_temp.iterrows():
# #     print('vehicle_id: {}\norder_id: {}\nuniversal_time: {}\nlongitude: {}\nlatitude: {}\n'.format(row["vehicle_id"], row["order_id"], row["universal_time"], 
# #                     row["longitude"], row["latitude"]))
# # print(gps_temp['vehicle_id'].value_counts())

# size = 20
# for n in range(0, order.count(), size):
#     if n+size>order.count():
#         size = order.count() % size
# gmap = gmplot.GoogleMapPlotter(gps["latitude"].mean(), gps["longitude"].mean(), 13)
# i = 1
# for order_id in order.head(30)['order_id']:
#     # order_id = order_instant['order_id']
#     gps_temp = gps.loc[gps['order_id'] == order_id]
#     # gmap = gmplot.GoogleMapPlotter(gps_temp["latitude"].mean(), gps_temp["longitude"].mean(), 13)

#     latitudes = gps_temp["latitude"]
#     longitudes = gps_temp["longitude"]

#     gmap.heatmap(latitudes, longitudes)

#     # order_temp = order.loc[order['order_id'] == order_id]
#     # dep_latitudes, dep_longitudes = order_temp["departure_lati"], order_temp["departure_longi"]
#     # arrival_latitudes, arrival_longitudes = order_temp["arrival_lati"], order_temp["arrival_longi"]

#     # gmap.scatter(dep_latitudes, dep_longitudes, 'cornflowerblue', size = 3)
#     # gmap.scatter(arrival_latitudes, arrival_longitudes, '#FF0000')
#     print('order-{}'.format(i))
#     # print('depart-{}'.format(order[]))
#     i+=1
# if not os.path.exists('./report'):
#     os.makedirs('./report')
# gmap.draw('./report/all_maps.html')

# order_id = 'eb9dd4095d9850e6287cefd813775a6c'
# order_1 = order.loc[order['order_id'] == order_id]
# gps_1 = gps.loc[gps['order_id'] == order_id]
# print('order: {}'.format(order_id))
# print('departure_time: {}'.format(order_1['departure_time']))
# print('universal_time: {}'.format(gps_1['universal_time']))
# print('difference: {}'.format(gps_1['universal_time']-order_1['departure_time']))
