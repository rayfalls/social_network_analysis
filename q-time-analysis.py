# -*- coding: utf-8 -*-
"""
Created on Mon Jun 05 09:25:25 2017

@author: zhangle1
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import preprocessing
from sklearn.cluster import KMeans



raw_dt_path = "C:/Users/zhangle1/Syncplicity Folders/python/egg/post_sum.csv"
time_zone_path = "C:/Users/zhangle1/Syncplicity Folders/python/egg/user_zone.csv"

raw_norm_path = os.path.normpath(raw_dt_path)
tz_nor_path = os.path.normpath(time_zone_path)


data_df = pd.read_csv(raw_norm_path,index_col=0,dtype={'quoted_authors':str})

post_time_df = data_df[['post_author','post_num','subpost_unix_time']]

post_time_df.loc[:,'subpost_unix_time'] = pd.to_datetime(post_time_df['subpost_unix_time'], unit='s')
post_time_df=post_time_df.set_index(['subpost_unix_time'])

#For all users
all_hour_grouped = post_time_df.groupby([post_time_df.index.hour])

# Group by user and hour
user_hour_grouped = post_time_df.groupby([post_time_df.index.hour, 'post_author']).count()
user_hour_grouped = user_hour_grouped.reset_index()
user_hour_grouped.rename(columns = {'level_0':'Hour'} , inplace = True)
all_user_pivot = user_hour_grouped.pivot(index = 'Hour', columns = 'post_author', values = 'post_num')

# Group by user and 15 min

user_quarter_grouped = post_time_df.groupby([pd.TimeGrouper(freq='15T'),'post_author']).count()
user_quarter_grouped.reset_index(inplace = True)
user_quarter_grouped.set_index('subpost_unix_time', inplace = True)

user_quarter_sum = user_quarter_grouped.groupby([user_quarter_grouped.index.hour, user_quarter_grouped.index.minute, 'post_author']).count()
user_quarter_sum.reset_index(inplace = True)
user_quarter_sum.rename(columns = {'level_0':'Hour', 'level_1':'Minute'}, inplace = True)

#################################################
# Predict user timezone by unsupervised learning#
#################################################

# Data table should be column: post_author, time of day lists, rows: author_id, n posts
user_quarter_sum['Hour'] = user_quarter_sum['Hour'].astype(str)
user_quarter_sum['Minute'] = user_quarter_sum['Minute'].astype(str)
user_quarter_sum.set_index('post_author', inplace = True)

user_quarter_pivot = pd.pivot_table(user_quarter_sum, values = 'post_num', index = user_quarter_sum.index,columns = ['Hour','Minute'])
user_quarter_pivot.fillna(0, inplace = True)
user_quarter_np = user_quarter_pivot.as_matrix()

# Data table manipulation, missing data handling

kmeans = KMeans(n_clusters=4, random_state=0).fit(user_quarter_np)
timezone = kmeans.labels_
timezone_s = pd.Series(data = timezone, name = 'Timezone')
new_df = pd.DataFrame(data = timezone_s.values, index = user_quarter_pivot.index)

# If we normalize it per row
normalized_X = preprocessing.normalize(user_quarter_np)
norm_kmeans = KMeans(n_clusters=4, random_state=0).fit(normalized_X)
timezone_norm = norm_kmeans.labels_
timezone_norm_s = pd.Series(data = timezone_norm, name = 'Timezone_Norm')
new_df = pd.DataFrame(data = [timezone_norm_s.values, timezone_s.values], index = user_quarter_pivot.index)
new_df.rename(columns = {0:'Numeric_rep'}, inplace = True)

string_rep = []

for values in new_df.Numeric_rep.values:
    if values == 0:
        string_rep.append('Eastern')
    elif values == 2:
        string_rep.append('Pacific')
    elif values == 1:
        string_rep.append('WTF')
    elif values == 3:
        string_rep.append('Inactive')

new_df = new_df.assign(String_rep = string_rep)
new_df.to_csv(tz_nor_path)

# Now It looks like west coast and east coast can be seperated out, how to identify center time zone?














