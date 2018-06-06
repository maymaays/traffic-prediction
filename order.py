import pandas as pd
from datetime import datetime
from gmplot import gmplot

order = pd.read_csv('./data/02a10/11-02/order_20161102', 
                    names = ["order_id", "departure_time", "arrival_time", 
                    "departure_longi", "departure_lati", "arrival_longi", "arrival_lati"])
order["departure_time"] = order["departure_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
order["arrival_time"] = order["arrival_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))


# for order_id in order['order_id']:
order_id = '982bf243c3202415d6252271b2693161'
order_temp = order.loc[order['order_id'] == order_id]

gmap = gmplot.GoogleMapPlotter(order_temp["departure_lati"].mean(), order_temp["departure_longi"].mean(), 13)

dep_latitudes, dep_longitudes = order_temp["departure_lati"], order_temp["departure_longi"]
arrival_latitudes, arrival_longitudes = order_temp["arrival_lati"], order_temp["arrival_longi"]

gmap.scatter(dep_latitudes, dep_longitudes, 'cornflowerblue', size = 3)
gmap.scatter(arrival_latitudes, arrival_longitudes, '#FF0000')

gmap.draw('{}.html'.format(str(order_id)))
