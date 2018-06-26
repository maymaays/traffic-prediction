from gmplot import gmplot

min_lati, min_longi = 30.65301, 104.0422
max_lati, max_longi = 30.72774, 104.12955

gmap = gmplot.GoogleMapPlotter((min_lati + max_lati) / 2, (min_longi + max_longi) / 2, 13)

# plot contour with polygon
lats, lons = zip(*[
    (min_lati, min_longi),
    (min_lati, max_longi),
    (max_lati, max_longi),
    (max_lati, min_longi),
    (min_lati, min_longi)
])

gmap.plot(lats, lons, 'cornflowerblue', edge_width=10)

gmap.draw('./contour_22.html')
