import os
import pandas as pd
from gmplot import gmplot

def read_file(file, columns):
    return pd.read_csv(file, names = columns)

order = read_file('./data/11-01/order_20161101',
                ['order_id', 'departure_time', 'arrival_time', 
                'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati'])
gps = read_file('./data/11-01/gps_20161101',
                ['vehicle_id', 'order_id', 'universal_time', 'longitude', 'latitude'])
order_ids = read_file('./weak_id/part-00000-ffdbafd4-793e-4c51-9ec5-4636cf13760c-c000.csv', ['order_id'])
# print(order_ids['order_id'])

base_path = './weak_heatmap'
if not os.path.exists(base_path):
    os.makedirs(base_path)
# gmap = gmplot.GoogleMapPlotter(gps["latitude"].mean(), gps["longitude"].mean(), 13)
i, j = 0, 0
for order_id in order_ids.sample(5)['order_id']:
    gps_temp = gps.loc[gps['order_id'] == order_id]
    lat, long = gps_temp['latitude'], gps_temp['longitude']
    gmap = gmplot.GoogleMapPlotter(gps_temp["latitude"].mean(), gps_temp["longitude"].mean(), 13)

    gmap.heatmap(lat, long)
    
    order_temp = order.loc[order['order_id'] == order_id]
    dep_latitudes, dep_longitudes = order_temp["departure_lati"], order_temp["departure_longi"]
    arrival_latitudes, arrival_longitudes = order_temp["arrival_lati"], order_temp["arrival_longi"]

    gmap.scatter(dep_latitudes, dep_longitudes, 'cornflowerblue', size = 3)
    gmap.scatter(arrival_latitudes, arrival_longitudes, '#FF0000')

    gmap.draw('{}/weak_heatmap_{}.html'.format(base_path, order_id))
    # i+=1
    # if i % 100 == 0:
    #     gmap.draw('{}/normal_heatmap_{}_{}.html'.format(base_path, j, i))
    #     gmap = gmplot.GoogleMapPlotter(gps["latitude"].mean(), gps["longitude"].mean(), 13)
    #     j = i
    #     break;
# gmap.draw('{}/normal_heatmap_{}_{}.html'.format(base_path, j, i))
