import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# data = pd.read_csv('./patt-num.csv')

# x = np.arange(4)
# ax = plt.subplot(212)
# plt.bar(x, height=(64471, 52777, 52292, 11632), color='#C7A0EA')
# plt.xticks(x + .04, ['normal', '(A-, B)', '(A, B-)', 'weak'])
# for i in ax.patches:
#     ax.text(i.get_x() + .2, i.get_height() - 3, \
#             str(round((i.get_height()), 2)), fontsize=10,
#                 color='#777887')

# ax = plt.subplot(211)
# ax.set_title("each pattern's percentage & count in day 1", fontsize=14)
# plt.bar(x, height=(35.56, 29.13, 28.86, 6.42), color='#ffb3ba')
# plt.xticks(x + .04, ['normal', '(A-, B)', '(A, B-)', 'weak'])
# for i in ax.patches:
#     ax.text(i.get_x() + .2, i.get_height() - 3, \
#             str(round((i.get_height() / 100) * 100, 2)) + '%', fontsize=10,
#                 color='#777887')

gps = pd.read_csv('./data/gps_hour_in_3_days.csv')

for i in range(3):
    # if i == 0:
    counts = [c for c in gps.iloc[i]]
    title = 'GPS Signals per Hour - Day {}'.format(i+1)
    #     counts = [c for c in gps.mean(axis=0)]
    #     title = 'Trip configuration\'s count - mean'
    # elif i == 1:
    #     counts = [c for c in gps.median(axis=0)]
    #     title = 'Trip configuration\'s count - median'
    # else:
    #     counts = [c for c in gps.var(axis=0)]
    #     title = 'Trip configuration\'s count - variance'
    x = np.arange(24)
    ax = plt.subplot(3, 1, i+1)
    ax.set_title(title, fontsize=12)
    plt.bar(x, height=tuple(counts[:-1]), color='#c4daea')
    plt.xticks(x + .025,
                ['0', '1', '2', '3', '4', '5', '6', '7', '8',
                '9', '10', '11', '12', '13', '14', '15', '16', '17',
                '18', '19', '20', '21', '22', '23'])
    for i in ax.patches:
        ax.text(i.get_x() + .05,
                i.get_height() - 5,
                str(round((i.get_height() / counts[-1] * 100), 2)) + '%',
                fontsize=5,
                color='#777887')

# x = np.arange(30)
# ax = plt.subplot(111)
# ax.set_title('GPS Signals per Day in November', fontsize=12)
# plt.bar(x, height=[c for c in gps.iloc[0]], color='#C7A0EA')
# plt.xticks(x + .025,
#             [t + 1 for t in range(30)])
plt.tight_layout()
plt.show()
