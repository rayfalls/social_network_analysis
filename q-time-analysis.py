# -*- coding: utf-8 -*-
"""
Created on Mon Jun 05 09:25:25 2017

@author: zhangle1
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn import preprocessing
from sklearn.cluster import KMeans



raw_dt_path = "/home/lei/Documents/python_workfolder/egg_updater/data/backup/post_sum.csv"
time_zone_path = "/home/lei/Documents/python_workfolder/egg_updater/data/backup/user_zone.csv"

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
user_hour_grouped.rename(columns = {'subpost_unix_time':'Hour_of_day'} , inplace = True)
all_user_pivot = user_hour_grouped.pivot(index = 'Hour_of_day', columns = 'post_author', values = 'post_num')

# Group by user and 15 min

user_quarter_grouped = post_time_df.groupby([pd.TimeGrouper(freq='15T'),'post_author']).count()
user_quarter_grouped.reset_index(inplace = True)
user_quarter_grouped.set_index('subpost_unix_time', inplace = True)

user_quarter_sum = user_quarter_grouped.groupby([user_quarter_grouped.index.hour, user_quarter_grouped.index.minute, 'post_author']).count()
user_quarter_sum.index.names = ['Hour','Minute','post_author']
user_quarter_sum.reset_index(inplace = True)

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
new_df = pd.DataFrame(data = {"normalized_pred":timezone_norm_s.values, "pred":timezone_s.values}, index = user_quarter_pivot.index)

string_rep = []

for values in new_df.normalized_pred.values:
    if values == 0:
        string_rep.append('Eastern')
    elif values == 2:
        string_rep.append('outlier?')
    elif values == 1:
        string_rep.append('Central')
    elif values == 3:
        string_rep.append('Pacific')

new_df = new_df.assign(String_rep = string_rep)
new_df.to_csv(tz_nor_path)

# Now It looks like west coast and east coast can be seperated out, how to identify center time zone?

# Data visualization
# Single user plot
def plot_bar_x(label,value):
    # this is for plotting purpose
    index = np.arange(len(label))
    plt.bar(index, value)
    plt.xlabel('Time of Day', fontsize=10)
    plt.ylabel('Activity', fontsize=10)
    plt.xticks(index, label, fontsize=10, rotation=45)
    plt.title('Aggregated user activity by time of day')
    
    fig = plt.gcf()
    fig.set_size_inches(40, 10)
    plt.show()

user_example = user_quarter_pivot.loc['yishizhu',:]
user_example = user_example.reset_index()
user_example['TOD'] = user_example.apply(lambda row: int(row.Hour) + (int(row.Minute) / 60), axis=1)
user_example['TOD_label'] = user_example.apply(lambda row: row.Hour + ":" + str(int(row.Minute)).zfill(2), axis=1)
user_example.sort_values(by=['TOD'],ascending = True, inplace = True)

label = user_example['TOD_label'].values.tolist()
value = user_example['yishizhu'].values.tolist()
plot_bar_x(label,value)

# All user by group
user_activity_group = pd.DataFrame(data = normalized_X, columns = label , index = user_quarter_pivot.index)
user_activity_group_merge = user_activity_group.merge(right = new_df, left_index = True, right_index = True)

user_activity_group_merge.set_index(['normalized_pred'], inplace = True)
user_activity_group_merge.drop(columns = ['pred','String_rep'],inplace = True)
user_data_for_stack = user_activity_group_merge.stack()
user_data_for_stack = user_data_for_stack.reset_index()
user_data_for_stack.rename(index=str,columns={'normalized_pred':'Group','level_1':'TOD',0:'Activity'}, inplace=True)
user_data_for_stack = user_data_for_stack[user_data_for_stack.Group !=2]

# Seaborn plot all users

sns.set(style="darkgrid")

# Plot the responses for different events and regions
plt.figure(figsize=(20, 10))
plt.xticks(fontsize=10, rotation=45)

sns.lineplot(x="TOD", y="Activity",
             hue="Group",
             data=user_data_for_stack,
             palette = sns.color_palette("muted",n_colors=3)
             )



