import pandas as pd
from datetime import datetime
from gmplot import gmplot

# order = pd.read_csv('./data/02a10/11-02/order_20161102', 
#                     names = ["order_id", "departure_time", "arrival_time", 
#                     "departure_longi", "departure_lati", "arrival_longi", "arrival_lati"])
# order["departure_time"] = order["departure_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))

gps = pd.read_csv('./data/02a10/11-02/gps_20161102',
                    names = ["vehicle_id", "order_id", "universal_time", 
                    "longitude", "latitude"])
# gps["universal_time"] = gps["universal_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# print(gps.head(5))
for vehicle_id in gps['vehicle_id']:
    gps_temp = gps.loc[gps['vehicle_id'] == vehicle_id]

    gmap = gmplot.GoogleMapPlotter(gps_temp["latitude"].mean(), gps_temp["longitude"].mean(), 13)

    latitudes = gps_temp["latitude"]
    longitudes = gps_temp["longitude"]

    gmap.heatmap(latitudes, longitudes)

    gmap.draw('{}.html'.format(vehicle_id))
