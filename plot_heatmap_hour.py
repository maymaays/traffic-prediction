import os
import pandas as pd
import folium
from folium import plugins

# read order file
def read_file(file, columns):
    return pd.read_csv(file, names=columns, sep='|')

gps = read_file('./data/gps_hour_data/22/gps_22_day2/part-00000-54099ff2-d33d-4466-80ce-26bd3cce3726-c000.csv',
                ["vehicle_id", "order_id", "universal_time", "longitude", "latitude"])
gps_arr = gps[['latitude', 'longitude']].as_matrix().tolist()

m = folium.Map([gps['latitude'].mean(), gps['longitude'].mean()], zoom_start=13)
m.add_children(plugins.HeatMap(gps_arr, radius=8))
m.save('./heatmap_day3.html')
