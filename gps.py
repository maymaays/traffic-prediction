import pandas as pd
from datetime import datetime
from gmplot import gmplot

# read order file
order = pd.read_csv('./data/02a10/11-02/order_20161102', 
                    names = ["order_id", "departure_time", "arrival_time", 
                    "departure_longi", "departure_lati", "arrival_longi", "arrival_lati"])
# order["departure_time"] = order["departure_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))

# read gps file
gps = pd.read_csv('./data/02a10/11-02/gps_20161102',
                    names = ["vehicle_id", "order_id", "universal_time", 
                    "longitude", "latitude"])
# gps["universal_time"] = gps["universal_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
# print(gps.head(5))
# for vehicle_id in gps['vehicle_id']:

order_id = '6681082cbcc11b37842489a096e2172e'
gps_temp = gps.loc[gps['order_id'] == order_id]

# for _, row in gps_temp.iterrows():
#     print('vehicle_id: {}\norder_id: {}\nuniversal_time: {}\nlongitude: {}\nlatitude: {}\n'.format(row["vehicle_id"], row["order_id"], row["universal_time"], 
#                     row["longitude"], row["latitude"]))

gmap = gmplot.GoogleMapPlotter(gps_temp["latitude"].mean(), gps_temp["longitude"].mean(), 13)

latitudes = gps_temp["latitude"]
longitudes = gps_temp["longitude"]

gmap.heatmap(latitudes, longitudes)

order_temp = order.loc[order['order_id'] == order_id]
dep_latitudes, dep_longitudes = order_temp["departure_lati"], order_temp["departure_longi"]
arrival_latitudes, arrival_longitudes = order_temp["arrival_lati"], order_temp["arrival_longi"]

gmap.scatter(dep_latitudes, dep_longitudes, 'cornflowerblue', size = 3)
gmap.scatter(arrival_latitudes, arrival_longitudes, '#FF0000')



gmap.draw('map-{}.html'.format(order_id))

