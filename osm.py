import osmapi as osm
import pandas as pd

order = pd.read_csv('./data/raw_data/11-01/order_20161101',
                    names=['order_id', 'depature_time', 'arrival_time',
                    'departure_longi', 'departure_lati', 'arrival_longi', 'arrival_lati'])
order = order.iloc[0]
api = osm.OsmApi(api='https://www.openstreetmap.org', passwordfile='./user.txt')

# print(api.NodeGet(123))

api.ChangesetCreate({'comment':'test'})

id1 = api.NodeCreate({'lat': order['departure_lati'], 'lon': order['departure_longi'], 'tag': {}})['id']
id2 = api.NodeCreate({'lat': order['arrival_lati'], 'lon': order['arrival_longi'], 'tag': {}})['id']

api.WayCreate({'nd': [id1, id2], 'tag': {}})

api.ChangesetClose()



