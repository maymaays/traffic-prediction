import os
import pandas as pd
import folium
from folium import plugins

# read order file
def read_file(file, columns):
    return pd.read_csv(file, names=columns, sep='|')

gps = read_file('./../data/gps_hour_data/23/gps_23_day1/part-00000-589e6d39-0c58-49d0-9ef1-f325da9348f6-c000.csv',
                ["vehicle_id", "order_id", "universal_time", "longitude", "latitude"])
gps_arr = gps[['latitude', 'longitude']].as_matrix().tolist()

m = folium.Map([gps['latitude'].mean(), gps['longitude'].mean()], zoom_start=13)
m.add_children(plugins.HeatMap(gps_arr, radius=8))
m.save('./heatmap_contour_day1.html')
