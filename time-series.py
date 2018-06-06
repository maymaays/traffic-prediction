import pandas as pd
from pandas import Series
from datetime import datetime
from matplotlib import pyplot

# series = Series.from_csv('./data/02a10/11-02/gps_20161102')

# print(series.head())

gps = pd.read_csv('./data/02a10/11-02/gps_20161102',
                    names = ["vehicle_id", "order_id", "universal_time", 
                    "longitude", "latitude"])
gps["universal_time"] = gps["universal_time"].apply(lambda t: datetime.fromtimestamp(t).strftime("%I:%M:%S"))
gps = gps.groupby('universal_time').count()
print(gps)
