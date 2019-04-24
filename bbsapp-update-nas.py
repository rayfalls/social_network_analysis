#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Created on Fri Dec 16 20:16:43 2016

@author: lzhan
"""
#This is the forum dataminer based on previous multi-thread implementation
#Process Flow
# 1. Initiation;
# 2. Look up previous indexed titles table determine break condition;
# 3. Look up title list, get a list of threads that needs to pull data;
# 4. Import and define parameters for data query engine;
# 5. Run the data query engine for each subpost with multi-thread processer
# 6. Post query merge main table
# 7. Post query merge update title table

#####################################################################
########################### 1. Dependency ###########################
#####################################################################
# Need the following .py script to run
# 1. subpost_spider.py
# 2. bbsapp_missing_file.py
# 3. bbsapp_missing_eggs.py
#####################################################################
########################### 1. Initiation ###########################
#####################################################################

import requests, os, time, sys
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from functools import partial

#Data paths:
work_folder = "/share2/CACHEDEV1_DATA/Public/python_workfolder/egg_updater/data/"
current_title_path = os.path.normpath(work_folder+"eggplant_titles.csv")
update_title_path = os.path.normpath(work_folder+"eggplant_titles_update.csv")
subpost_temp_path = work_folder+"subpost/"
current_data_path = os.path.normpath(work_folder+"post_sum.csv")
updated_data_path = os.path.normpath(work_folder+"post_sum_update.csv")

#Parameters for log in website
username = "username"
password = "password"
login_url = 'https://bbsapp.org/index.php?login/login'
payload = {
           'login': username, 
           'register': '0',
           'password': password,
           'remember': '1',
           'cookie_check': '1',
           '_xfToken': ' ',
           'redirect': '/index.php'}
           
request_headers = {
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Accept-Encoding' : 'gzip, deflate',
                   'Accept-Language' : 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2',
                   'Cache-Control': 'max-age=0',
                   'Connection' : 'Keep-Alive',
                   'Content-Length' : '89',
                   'Content-Type' : 'application/x-www-form-urlencoded',
                   'Cookie': 'xf_session=c05fdbb20fc1d0925681c9ec5fb0dcc1; hibext_instdsigdip=1; _ga=GA1.2.1893741541.1503031697; _gid=GA1.2.734073486.1503031697; _gat=1',
                   'Host' : 'bbsapp.org',
                   'Origin': 'https://bbsapp.org',
                   'Referer': 'https://bbsapp.org/index.php',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
                   }

#Log in website and validate session have logged in.
s = requests.session()

#Login attempt
login_attempt = s.post(login_url, data = payload, headers = request_headers)


#Access user file attempt
eg_access = s.get("https://bbsapp.org/index.php?members/5/")

#Access post attempt
eg_post = s.get('https://bbsapp.org/index.php?threads/2/')

print(login_attempt.status_code)
print(eg_access.status_code)
print(eg_post.status_code)
if login_attempt.status_code == 403:
    sys.exit("Whoops, log in failed")

#####################################################################
##### 2/3. Read Existing Title Data to determine new query range ####
#####################################################################

# Read last title file
last_titles_pd = pd.read_csv(current_title_path, index_col = 0)

# Get time stamp of newest post in previous query
last_post_time = pd.to_datetime(last_titles_pd.exact_time).max()

# Start to query titles page

eg_front = s.get('https://www.bbsapp.org/index.php?forums/2/')
print(eg_front.status_code)

soup_front = BeautifulSoup(eg_front.text, 'html.parser')

thread_id, title_text, title_text, prefix_id, author_id, member_id, post_time, thread_link, n_response, n_views = ([] for i in range(0,10))

#Beautiful soup search and extract infomation on title page
title_header = np.array([['thread_id','prefix_id','title_text','author_id','member_id','post_time','thread_link','n_response','n_views']])

#Find out how many pages are there
last_page_s = soup_front.find(class_='PageNav').get('data-last')
last_page = int(last_page_s)

#Iterate over all pages to get a post list
for pnum in range(1,last_page+1):
    exitFlag = False
    title_page_link = 'https://www.bbsapp.org/index.php?forums/2/page-' + str(pnum)
    print(title_page_link)
    eg_tpages= s.get(title_page_link)
    soup_tpages = BeautifulSoup(eg_tpages.text, 'html.parser')
    
    #Beautiful soup to extract each page    
    for title in soup_tpages.find(name='ol',class_='discussionListItems').find_all('li'):
        # Construct the break statement, two factors check, 1: if it is a sticky post; 2: if the post time is older than current data
        # 1. Sticky check
        non_sticky_check = type(title.find(class_='sticky'))== type(None)
        # 2. Time stamp check
        # Get post date, there are two scenarios or more, if the tag abbr exist, then it is possible to get unix
        # time string, if the tag span exists, then it is possible to get the time by getting the text of the tag
        if type(title.find(class_='dateTime')) != type(None):
            if type(title.find(class_='dateTime').find('abbr')) != type(None):
                title_nix_time = title.find(class_='dateTime').abbr.get('data-time')
            elif type(title.find(class_='dateTime').find('span')) != type(None):
                title_str_time = title.find(class_='dateTime').span.get('title').encode('utf-8')
                title_time= str(title_str_time[:10] + b' ' + title_str_time[-5:])
                title_strptime = time.strptime(title_time,b'%Y-%m-%d %H:%M')
                title_nix_time = time.mktime(title_strptime)
        else:
            title_nix_time = 'None'
        title_pd_time = pd.to_datetime(str(title_nix_time).encode('utf8'), unit='s')
        # When post time is older than previous max, then data already exist in master file and query can stop
        time_stamp_check = last_post_time > title_pd_time
        
        #break statement identifier
        exitFlag = non_sticky_check & time_stamp_check
        if exitFlag:
            break
        thread_id.append(title.get('id'))
        author_id.append(title.get('data-author'))
        member_id.append(title.find(class_='posterDate').a.get('href'))
        post_time.append(title_nix_time)
        thread_link.append(title.find(class_='startDate').a.get('href'))
        n_response.append(title.find(class_='stats').find(class_='major').find('dd').get_text())
        title_text.append(title.find(class_='title').a.getText())
        n_views.append(title.find(class_='stats').find(class_='minor').find('dd').get_text())

        
        #Find prefix_id:
        try:
            prefix_id.append(title.find(class_='title').find(class_='prefixLink').get('href'))
        except AttributeError:
            prefix_id.append('None')
    if exitFlag:
        break

#Print out len of the lists for sanity check
print(len(thread_id))
print(len(prefix_id))
print(len(author_id))
print(len(member_id))
print(len(post_time))
print(len(thread_link))
print(len(title_text))

#Encoding to handle chinese characters
thread_id_utf8 = [thread.encode('utf8') for thread in thread_id]
prefix_id_utf8 = [prefix.encode('utf8') for prefix in prefix_id]
author_id_utf8 = [author.encode('utf8') for author in author_id]
thread_link_utf8 = [t_link.encode('utf8') for t_link in thread_link]
n_response_utf8 = [n_res.encode('utf8') for n_res in n_response]                   
n_views_utf8 = [n_view.encode('utf8') for n_view in n_views]
title_text_utf8 = [ttx.encode('utf8') for ttx in title_text]
                
member_id_utf8 =[]
p_time_utf8 = []

for member in member_id:
	try:
		member_id_utf8.append(member.encode('utf8') )
	except AttributeError:
		member_id_utf8.append('None')

for p_time in post_time:
	try:
		p_time_utf8.append(str(p_time).encode('utf8') )
	except AttributeError:
		p_time_utf8.append('None')
#Trying to set up some other parameters

#Get exact time
exact_time_temp = []
exact_time_unavailable = [pt_utf8 =='None' for pt_utf8 in p_time_utf8]
for i in range(0,len(exact_time_unavailable)):
    if exact_time_unavailable[i]:
        exact_time = pd.NaT
    else:
        exact_time = pd.to_datetime(p_time_utf8[i], unit='s')
    exact_time_temp.append(exact_time)
exact_time_pd = pd.Series(exact_time_temp)

#End

#New Header list after addition of new parameters
header_list = title_header[0,:].tolist()

#Format and save
title_utf8 = thread_id_utf8 + prefix_id_utf8 + title_text + author_id_utf8 + member_id_utf8 + p_time_utf8 + thread_link_utf8 + n_response_utf8 + n_views_utf8
title_np = np.array(title_utf8).reshape((9,len(thread_id)))
title_transpose = np.transpose(title_np)
title_pd = pd.DataFrame(title_transpose,columns = header_list)

#Now add the newly added values to the dataframe
title_pd = title_pd.assign(exact_time = exact_time_pd.values)

title_pd.reset_index(inplace = True)
title_pd.set_index('thread_id',inplace = True)
title_pd.drop('index',1, inplace = True)
title_pd.to_csv(update_title_path, encoding='utf-8')  
            
            
#####################################################################
#######End Read Existing Title Data to determine new query range ####
#####################################################################



#####################################################################
######################### 4.Sub Post Engine #########################
#####################################################################

update_titles_pd = pd.read_csv(update_title_path, index_col = 0)
thread_link_list = update_titles_pd.thread_link.tolist()
import subpost_spider

#####################################################################
########################End Sub Post Engine #########################
#####################################################################    

#####################################################################
##################### 5. Multi-thread processer #####################
#####################################################################    

#multithread processer
start = time.time()
spider_partial = partial(subpost_spider.subpost_spider, thread_link_list = thread_link_list, title_pd = title_pd, subpost_temp_path = subpost_temp_path)
results = ThreadPool(5).imap_unordered(spider_partial, thread_link_list)
for sublink in results:
    print("%r fetched in %ss" % (sublink, time.time() - start))
    
# Check if number of files match len(thread_link_list), if not, find missing and repeat
import exception_handler
subfiles_folder_path = os.path.normpath(work_folder+"subpost/")
n_output = len(os.listdir(subfiles_folder_path))
n_input = len(thread_link_list)

while(n_output != n_input):
    exception_handler.find_missing(work_folder)
    exception_handler.missing_file_repeat(work_folder,payload)
    n_output = len(os.listdir(subfiles_folder_path))
#####################################################################
###################End Multi-thread processer #######################
#####################################################################    


#####################################################################
######################## 6.  CSV Merger #############################
##################################################################### 

subpost_temp_folder = os.path.normpath(subpost_temp_path)

sub_post_files = os.listdir(subpost_temp_folder)

post_sum = pd.DataFrame()

for sub_post_file_name in sub_post_files:
    sub_post_indifile_path = os.path.normpath(subpost_temp_path + sub_post_file_name)
    sub_post_indifile = pd.read_csv(sub_post_indifile_path, index_col = 0)
    post_sum = post_sum.append(sub_post_indifile, ignore_index=True)

post_sum.to_csv(updated_data_path)

## Clear subfiles folder
sub_post_files = os.listdir(subpost_temp_folder)
for file_name in sub_post_files:
    os.remove(os.path.normpath(subpost_temp_path + file_name))

#####################################################################
######################End CSV Merger ################################
##################################################################### 

#####################################################################
################### Master File Update ###############################
##################################################################### 

#Previous post_sum

current_post_sum = pd.read_csv(current_data_path, index_col = 0)


#New post_sum pd to be merged
update_post_sum = pd.read_csv(updated_data_path, index_col = 0)
new_data_pd = pd.concat([current_post_sum , update_post_sum])
new_data_pd.drop_duplicates(subset = ['thread_id', 'post_num','quoted_thread' ],keep = 'last', inplace = True)
new_data_pd.reset_index(inplace = True)
new_data_pd.reindex()
new_data_pd.drop('index',1, inplace = True)
new_data_pd.to_csv(current_data_path)


#####################################################################
################### End Master File Update ##########################
##################################################################### 

#####################################################################
################### Title File Update ###############################
##################################################################### 

#Previous title pd
last_titles_pd = pd.read_csv(current_title_path, index_col = 0)


#New title pd to be merged
title_pd = pd.read_csv(update_title_path, index_col = 0)


# Concat title pd and remove duplicate

new_titles_pd = pd.concat([last_titles_pd , title_pd])
new_titles_pd.reset_index(inplace = True)
new_titles_pd.drop_duplicates(subset = 'thread_id', keep = 'last', inplace = True)
new_titles_pd.reset_index(inplace = True)
new_titles_pd.set_index('thread_id', inplace = True)
new_titles_pd.drop('index',1, inplace = True)
new_titles_pd.to_csv(current_title_path)

#####################################################################
################### End Title File Update ###########################
##################################################################### 