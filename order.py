import pandas as pd
from datetime import datetime
from gmplot import gmplot

# read data from csv file and set all column names
order = pd.read_csv('./data/02a10/11-02/order_20161102', 
                    names = ["order_id", "departure_time", "arrival_time", 
                    "departure_longi", "departure_lati", "arrival_longi", "arrival_lati"])
# set departure-arrival time into hh:mm:ss form 
order["departure_time"] = order["departure_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))

gmap = gmplot.GoogleMapPlotter(order["departure_lati"].mean(), order["departure_longi"].mean(), 13)
# duration = order["departure_time"] - order["arrival_time"]
for order_id in order.head(2)['order_id']:

# set order to specific id
    # order_id = '5085b204936c381e4e25566760667f17'
    order_temp = order.loc[order['order_id'] == order_id]

    # gmap = gmplot.GoogleMapPlotter(order_temp["departure_lati"].mean(), order_temp["departure_longi"].mean(), 13)

    dep_latitudes, dep_longitudes = order_temp["departure_lati"], order_temp["departure_longi"]
    arrival_latitudes, arrival_longitudes = order_temp["arrival_lati"], order_temp["arrival_longi"]

    gmap.scatter(dep_latitudes, dep_longitudes, 'cornflowerblue', size = 3)
    gmap.scatter(arrival_latitudes, arrival_longitudes, '#FF0000')

    print('{}: {} - {}'.format(order_id, order_temp.iloc[0]["departure_time"], order_temp.iloc[0]["arrival_time"]))

gmap.draw('order.html')
