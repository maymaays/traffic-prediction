import os
import pandas as pd
import folium
from folium import plugins

# read order file
def read_file(file, columns):
    return pd.read_csv(file, names = columns)

order_ids = read_file('./data/pattern_data/normal_heatmap/day_1/normal_id/part-00000-68b7478c-9873-4efe-aab7-8980a0740cee-c000.csv',
                    ['order_id'])
order = read_file('./data/raw_data/11-01/order_20161101',
                ['order_id', 'depature_time', 'arrival_time', 'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati'])
gps = read_file('./data/raw_data/11-01/gps_20161101',
                ["vehicle_id", "order_id", "universal_time", "longitude", "latitude"])
i = 1
for order_id in order_ids.sample(3)['order_id']:
    o = order.loc[order['order_id'] == order_id]
    g = gps.loc[gps['order_id'] == order_id]
    gps_arr = g[['latitude', 'longitude']].as_matrix().tolist()

    m = folium.Map([g['latitude'].mean(), g['longitude'].mean()], zoom_start=13)
    folium.Marker([o.iloc[0]['departure_lati'], o.iloc[0]['departure_longi']], icon=folium.Icon(color='blue')).add_to(m)
    folium.Marker([o.iloc[0]['arrival_lati'], o.iloc[0]['arrival_longi']], icon=folium.Icon(color='red')).add_to(m)
    m.add_children(plugins.HeatMap(gps_arr, radius=8))
    m.save('./heatmap_{}.html'.format(i))
    i+=1
