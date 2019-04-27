# -*- coding: utf-8 -*-
"""
Created on Fri Jun 02 09:07:01 2017

@author: zhangle1
"""

import os
import pandas as pd
import numpy as np
import networkx as nx


raw_dt_path = "C:/Users/zhangle1/Syncplicity/python/egg/post_sum.csv"

raw_norm_path = os.path.normpath(raw_dt_path)
graph_output = os.path.normpath('C:/Users/zhangle1/Syncplicity/python/egg/text2.gexf')

data_df = pd.read_csv(raw_norm_path,index_col=0,dtype={'quoted_authors':str})



# Pair Stats
data_group = data_df.groupby(['post_author','quoted_author'])

                               
pair_stats_nw = data_group.count()['author_id']
pair_stats_nw_filter = pair_stats_nw[pair_stats_nw.index.get_level_values(1) != 'Not_A_User']
pair_stats = pair_stats_nw_filter.reset_index()

pair_stats.rename( columns = {'author_id':'reply_fwd'}, inplace = True)


# Summerized stats by single user
author_post_count = pair_stats.groupby(['post_author']).sum()
author_post_count = author_post_count.reset_index()
author_post_count.rename( columns = {'reply_fwd':'post_total'}, inplace = True)

# Filter out inactive users (post<395)
inactive = author_post_count[author_post_count['post_total' ]<395]
inactive_list = inactive['post_author'].tolist()
pair_stats.drop(pair_stats[pair_stats['post_author'].isin(inactive_list)].index, inplace = True)
pair_stats.drop(pair_stats[pair_stats['quoted_author'].isin(inactive_list)].index, inplace = True)
pair_stats.drop(pair_stats[pair_stats['post_author'] == pair_stats['quoted_author']].index, inplace = True)



# Merge pair_stats with itself, and single stats

merge = pd.merge(pair_stats,pair_stats,how = 'inner', left_on = ['post_author','quoted_author'], right_on = ['quoted_author','post_author'])
merge.rename( columns = {'reply_fwd_y':'reply_bwd'}, inplace = True)

merge = pd.merge(merge,author_post_count,how = 'inner', left_on = ['post_author_x'], right_on = ['post_author'])
merge.drop(['post_author_y','quoted_author_y','post_author'], axis = 1, inplace = True)
merge.rename( columns = {'post_total':'post_total_fwd'}, inplace = True)

merge = pd.merge(merge,author_post_count,how = 'inner', left_on = ['quoted_author_x'], right_on = ['post_author'])
merge.drop(['post_author'], axis = 1, inplace = True)
merge.rename( columns = {'post_total':'post_total_bwd'}, inplace = True)

interaction_index = pow(pow((merge['reply_fwd_x']/merge['post_total_fwd']),2)+pow((merge['reply_bwd']/merge['post_total_bwd']),2),0.5).tolist()
merge['interaction_index'] = np.array(interaction_index)

fwd_weight = (merge['reply_fwd_x']/merge['post_total_fwd']).tolist()
bwd_weight = (merge['reply_bwd']/merge['post_total_bwd']).tolist()
merge['fwd_weight'] = np.array(fwd_weight)
merge['bwd_weight'] = np.array(bwd_weight)
average_weight = ((merge['fwd_weight'] + merge['bwd_weight'])/2).tolist()
merge['average_weight'] = np.array(average_weight)


merge_highindex = merge[merge['interaction_index' ]> 0.01]

# Networkx
# DG1
DG1=nx.DiGraph()

user_list = merge['post_author_x'].tolist()
DG1.add_nodes_from(user_list)

edge_list = [(merge['post_author_x'].iloc[i], merge['quoted_author_x'].iloc[i],merge['fwd_weight'].iloc[i] ) for i in range(0,merge.shape[0])]
DG1.add_weighted_edges_from(edge_list)

nx.write_gexf(DG1,graph_output)

# DG2
DG2=nx.DiGraph()

user_list = merge_highindex['post_author_x'].tolist()
DG2.add_nodes_from(user_list)

edge_list = [(merge_highindex['post_author_x'].iloc[i], merge_highindex['quoted_author_x'].iloc[i],merge_highindex['fwd_weight'].iloc[i] ) for i in range(0,merge_highindex.shape[0])]
DG2.add_weighted_edges_from(edge_list)

nx.write_gexf(DG2,graph_output)

# With the network file created, additional network visualization is done in interactive open source tool gephi (gephi.org)













