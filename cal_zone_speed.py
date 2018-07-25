import math
import os
import pandas as pd
from datetime import datetime
from haversine import haversine
from itertools import chain
from statistics import median

def second_to_timestamp(df, format):
    return df.apply(lambda t: int(datetime.fromtimestamp(t).strftime(format)))

path = './zone/zones_table_min_10.csv'
day = 15

for i in range(20):
    result_df = pd.DataFrame(columns=['Zone', 'MedianSpeed', 'Day', 'Time', '#Vehicles', '#Orders'])
    zone = i+1
    print(zone)
    directory = './zone/day{}_zone1_20/gps_zone{}'.format(day, zone)
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            gps = pd.read_csv(os.path.join(directory, file),
                            sep='|',
                            names=['vehicle_id', 'order_id', 'universal_time', 'longitude', 'latitude'])
    gps['hour'] = (second_to_timestamp(gps['universal_time'], '%-H') + 8) % 24
    gps['minute'] = second_to_timestamp(gps['universal_time'], '%M')
    gps['second'] = second_to_timestamp(gps['universal_time'], '%S')
    if day == 1:
        for hour in range(0, 8):
            print(day, hour)
            for minute in range(0, 60, 5):
                start, end = minute, minute+4
                result_df = result_df.append(pd.DataFrame(data={
                    'Zone': [zone],
                    'MedianSpeed': [0],
                    'Day': [day],
                    'Time': ['{0:02}:{1:02}:00-{0:02}:{2:02}:59'.format(hour, start, end)],
                    '#Vehicles': [0],
                    '#Orders': [0]
                })[['Zone', 'MedianSpeed', 'Day', 'Time', '#Vehicles', '#Orders']])
    for hour in chain(range(8, 24), range(0, 8)):
        output_day = day
        if hour < 8:
            output_day+=1
        print(output_day, hour)
        for minute in range(0, 60, 5):
            start, end = minute, minute+4
            gps_temp_1 = gps.query('hour == {} & minute >= {} & minute <= {}'.format(hour, start, end))
            speeds = []
            for vehicle_id in gps_temp_1['vehicle_id'].drop_duplicates():
                gps_temp_2 = gps_temp_1.loc[gps_temp_1['vehicle_id'] == vehicle_id]
                dist = 0
                for j in range(len(gps_temp_2)-1):
                    lat1, long1 = gps_temp_2.iloc[j]['latitude'], gps_temp_2.iloc[j]['longitude']
                    lat2, long2 = gps_temp_2.iloc[j+1]['latitude'], gps_temp_2.iloc[j+1]['longitude']
                    dist+=haversine((lat1, long2), (lat2, long2))
                time = (gps_temp_2.loc[gps_temp_2['universal_time'].idxmax()]['universal_time']-gps_temp_2.loc[gps_temp_2['universal_time'].idxmin()]['universal_time'])/3600
                speed = dist/time
                if not math.isnan(speed) and not math.isinf(speed) and speed >= 10:
                    speeds.append(speed)
            try:
                median_speed = median(speeds)
            except:
                median_speed = 0

            result_df = result_df.append(pd.DataFrame(data={
                'Zone': [zone],
                'MedianSpeed': [median_speed],
                'Day': [output_day],
                'Time': ['{0:02}:{1:02}:00-{0:02}:{2:02}:59'.format(hour, start, end)],
                '#Vehicles': [gps_temp_1['vehicle_id'].nunique()],
                '#Orders': [gps_temp_1['order_id'].nunique()]
            })[['Zone', 'MedianSpeed', 'Day', 'Time', '#Vehicles', '#Orders']])
    
    if not os.path.exists(path):
        result_df.reset_index(drop=True).to_csv(path, index=False)
    else:
        pd.read_csv(path).append(result_df).reset_index(drop=True).to_csv(path, index=False)
