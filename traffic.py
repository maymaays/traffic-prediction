import pandas as pd
from datetime import datetime
from gmplot import gmplot

# order = pd.read_csv('./02a10/11-02/order_20161102', 
#                     names = ["order_id", "departure_time", "arrival_time", 
#                     "departure_longi", "departure_lati", "arrival_longi", "arrival_lati"])
# order["departure_time"] = order["departure_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))

gps = pd.read_csv('./02a10/11-02/gps_20161102',
                    names = ["vehicle_id", "order_id", "universal_time", 
                    "longitude", "latitude"])
# gps["universal_time"] = gps["universal_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# print(gps.head(5))
gps = gps.loc[gps['vehicle_id'] == 'a739b90e4907fa30b0d6a3a3b39e67bb']

gmap = gmplot.GoogleMapPlotter(gps["latitude"].mean(), gps["longitude"].mean(), 13)

latitudes = gps["latitude"]
longitudes = gps["longitude"]

gmap.scatter(latitudes, longitudes, 'cornflowerblue', size = 10)

# gmap.heatmap(latitudes, longitudes)

gmap.draw("my_map.html")
