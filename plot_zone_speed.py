import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

zones = pd.read_csv('./zone/zones_table_min_10.csv')

day = 1
for i in range(20):
    zone = i+1
    z = zones.query('Day == {} & Zone == {}'.format(day, zone)).reset_index(drop=True)
    plt.suptitle('Zone {}'.format(zone))
    plt.plot(z['MedianSpeed'], color='#ffc0cb')
    plt.plot(z['MedianSpeed'], 'ro', color='#8394de', markersize=1.5)
    plt.xlabel('Time')
    plt.ylabel('Median Speed')
    plt.xticks(np.arange(0, 288, 12), [h for h in range(24)])
    plt.gca().set_xticks(np.arange(0, 288, 3), minor=True)
    plt.yticks(np.arange(0, 60, 5))
    plt.gca().set_yticks(np.arange(0, 60, 1), minor=True)
    path = './zone/figs_min'
    if not os.path.exists(path):
        os.mkdir(path)
    plt.savefig('{}/plot_{}'.format(path, zone))
    plt.clf()
    # plt.show()
