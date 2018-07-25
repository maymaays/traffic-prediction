from gmplot import gmplot

min_lati, min_longi = 30.65301, 104.0422
max_lati, max_longi = 30.72774, 104.12955
diff_lati, diff_long = (max_lati-min_lati)/4, (max_longi-min_longi)/5

gmap = gmplot.GoogleMapPlotter((min_lati + max_lati) / 2, (min_longi + max_longi) / 2, 13)

# plot contour with polygon
lats, lons = zip(*[
    (min_lati, min_longi),
    (min_lati, max_longi),
    (max_lati, max_longi),
    (max_lati, min_longi),
    (min_lati, min_longi)
])

start_lati, start_long = max_lati, min_longi
for lat_i in range(4):
    for long_i in range(5):
        zone_lats, zone_lons = zip(*[
            (start_lati, start_long),
            (start_lati, start_long + diff_long),
            (start_lati - diff_lati, start_long + diff_long),
            (start_lati - diff_lati, start_long),
            (start_lati, start_long)
        ])
        gmap.plot(zone_lats, zone_lons, 'cornflowerblue', edge_width=5)
        print('Latitude: {} - {}'.format(start_lati, start_lati - diff_lati))
        print('Longitude: {} - {}'.format(start_long, start_long + diff_long))
        start_long += diff_long
    start_long = min_longi
    start_lati -= diff_lati

gmap.plot(lats, lons, 'cornflowerblue', edge_width=10)

gmap.draw('./contour_test.html')
