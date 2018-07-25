import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xgboost

from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor

def create_query(days):
    query = 'Day == {}'.format(days[0])
    for i in range(1, len(test_days)):
        query+=' or Day == {}'.format(days[i])
    return query

drop = ['Zone', 'Day', 'Time']
normalize = ['#Vehicles', '#Orders']
target = ['MedianSpeed']

zones = pd.read_csv('./zone/zones_table_min_10.csv')
for zone_i in range(0, 20):
    zone = zone_i+1
    zones['Zone{}'.format(zone)] = zones['Zone'].apply(lambda x: zone == x)
for hour in range(0, 24):
    zones['Hour{}'.format(hour)] = zones['Time'].apply(lambda x: hour == int(x.split('-')[0].split(':')[0]))
for minute in range(0, 60, 5):
    zones['Minute{}-{}'.format(minute, minute+5)] = zones['Time'].apply(lambda x: minute == int(x.split('-')[0].split(':')[1]) and minute+4 == int(x.split('-')[1].split(':')[1]))
for feature in normalize:
    zones[feature] = (zones[feature]-zones[feature].min())/(zones[feature].max()-zones[feature].min())

days = list(range(2, zones.loc[zones['Day'].idxmax()]['Day']))
test_days = [days[i] for i in random.sample(range(len(days)), int(len(days)*.3))]
train_days = list(set(days) - set(test_days))
query_1 = create_query(train_days)
query_2 = create_query(test_days)
train, test = zones.query(query_1), zones.query(query_2)
X_train, y_train = train.drop(drop + target, axis=1), train[target]
X_test, y_test = test.drop(drop + target, axis=1), test[target]

params = {'n_estimators': 500,
        'max_depth': 4,
        'min_samples_split': 2,
        'learning_rate': .01,
        'loss': 'ls'}
clf = XGBRegressor(**params)
clf.fit(X_train, y_train)
predictions = clf.predict(X_test)
mse = mean_squared_error(y_test, predictions)
print('MSE: {}'.format(mse))

steps = 10
predictions, y_test = predictions[0::steps], y_test.values[0::steps]
size = len(predictions)
for idx, (vals, name, color) in enumerate([(predictions, 'Prediction', '#d896ff'), (y_test, 'Actual', '#ffaaa5')]):
    ax = plt.subplot(2, 1, idx+1)
    ax.set_title(name)
    plt.plot(range(size), vals, lw=.75, color=color)
    plt.yticks(np.arange(0, 90, 15))
plt.tight_layout()
plt.savefig('./zone/xgboost.png')

# for prediction, actual in zip(predictions, y_test.values):
#     print('Pred: {:5.2f}, Actual: {:5.2f}'.format(prediction, actual[0]))

# fig, ax = plt.subplots()
# xgboost.plot_importance(clf, max_num_features=10, height=0.8, ax=ax)
# plt.tight_layout()
# plt.show()

# test_score = np.zeros((params['n_estimators']), dtype=np.float64)
# for i, y_pred in enumerate(clf.staged_predict(X_test)):
#     test_score[i] = clf.loss_(y_test.values, y_pred)
# plt.figure(figsize=(12, 6))
# plt.title('Deviance')
# plt.plot(np.arange(params['n_estimators'])+1, clf.train_score_, 'b-', label='Training Set Deviance')
# plt.plot(np.arange(params['n_estimators'])+1, test_score, 'r-', label='Test Set Deviance')
# plt.legend(loc='upper right')
# plt.xlabel('Boosting Iterations')
# plt.ylabel('Deviance')
# plt.show()
