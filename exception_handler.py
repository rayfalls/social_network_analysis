#finding the missing piece
import os, re, requests, time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

def find_missing(work_folder,payload):
    #work_folder = "C:/Users/Tony/Desktop/test/"
    #work_folder = "/share2/CACHEDEV1_DATA/Public/python_workfolder/egg_updater/data/"
    csv_path = os.path.normpath(work_folder +"eggplant_titles_update.csv")
    missing_eggs_path = os.path.normpath(work_folder +"missing_eggs.csv")
    subfiles_folder_path = os.path.normpath(work_folder +"subpost/")
    test_csv_path = os.path.normpath(work_folder +"eggplant_titles_test.csv")  
    sub_post_files = os.listdir(subfiles_folder_path)
    
    title_pd = pd.read_csv(csv_path,index_col=[0])
    
    sub_post_files_reformat = ['threads/' + individual_file[7:individual_file.find("_")]+ '/' for individual_file in sub_post_files]
    thread_link_list = title_pd['thread_link'].tolist()
    
    comparison = []
    for sub_thread_link_single in thread_link_list:
        try:
            comparison.append(sub_post_files_reformat.index(sub_thread_link_single[sub_thread_link_single.find("?")+1:]))
        except ValueError:
            comparison.append(-1)
    
    title_df_new = title_pd.assign(comparison_result = comparison)
    missing_eggs_df = title_df_new.loc[title_df_new['comparison_result'] == -1]
    title_df_new.to_csv(test_csv_path,encoding='utf-8')
    missing_eggs_df.to_csv(missing_eggs_path,encoding='utf-8')

if __name__ == "__find_missing__":
    find_missing()
    
def missing_file_repeat(work_folder):
    #work_folder = "C:/Users/Tony/Desktop/test/"
    #work_folder = "/share2/CACHEDEV1_DATA/Public/python_workfolder/egg_updater/data/"
    
    missing_csv_path = os.path.normpath(work_folder +"missing_eggs.csv")
    ms_eggs_df = pd.read_csv(missing_csv_path)
    
    
    #Parameters
    login_url = 'https://bbsapp.org/index.php?login/login'

    #Save data link:
    current_title_path = os.path.normpath(work_folder+"eggplant_titles.csv")
    update_title_path = os.path.normpath(work_folder+"eggplant_titles_update.csv")
    subpost_temp_path = work_folder+"subpost/"
    
    current_data_path = os.path.normpath(work_folder+"post_sum.csv")
    updated_data_path = os.path.normpath(work_folder+"post_sum_update.csv")
    
    updated_data_path = os.path.normpath(work_folder+"post_sum_update.csv")
               
    #Try Set Headers
    
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
    
    
    s = requests.session()
    
    
    #Login attempt
    eggplant_try = s.post(login_url, data = payload, headers = request_headers)
    
    
    #Access user file attempt
    eg_access = s.get("https://bbsapp.org/index.php?members/xau.5/")
    
    #Access post attempt
    eg_post = s.get('https://bbsapp.org/index.php?threads/%E6%9C%AC%E7%AB%99%E7%89%88%E8%A7%84.2/')
    
    
    print(eggplant_try.status_code)
    print(eg_access.status_code)
    print(eg_post.status_code)
    
    thread_link_list = ms_eggs_df['thread_link'].tolist()
    title_pd = pd.read_csv(update_title_path, index_col = 0)
    thread_link_list_master = title_pd['thread_link'].tolist()
    
    def subpost_spider(sublink):
        post_link = 'https://www.bbsapp.org/' + sublink
        #post_link = 'https://www.bbsapp.org/threads/148273'
        eg_posts= s.get(post_link)
        soup_posts = BeautifulSoup(eg_posts.text, 'html.parser')
        
        #Get # of pages for this post.
        if type(soup_posts.find(class_='PageNav')) == type(None):
            n_pages = 1
        elif type(soup_posts.find(class_='PageNav')) != type(None) and type(soup_posts.find(class_='PageNav').get('data-last')) == type(None):
            n_pages = 1
        elif type(soup_posts.find(class_='PageNav')) != type(None) and type(soup_posts.find(class_='PageNav').get('data-last')) != type(None):
            n_pages = soup_posts.find(class_='PageNav').get('data-last')
    
        #Iterate over all pages to extract post info
        post_author = []
        post_num = []
        subpost_unix_time = []
        post_seq = []
        like_tag_list = []
        like_tag_nums = []
        like_tag_users = []
        quote_numbers = []
        quoted_author = []
        quoted_thread = []
        post_body = []
        
        for i in range (1, int(n_pages)+1):
            pages_link = 'https://www.bbsapp.org/' + sublink +'page-' + str(i)
            eg_pagess= s.get(pages_link)
            soup_pagess = BeautifulSoup(eg_pagess.text, 'html.parser')
            for subpost in soup_pagess.find(name = 'ol', class_ = 'messageList').find_all(name = 'li',class_ = 'message',recursive = False):
                if subpost.a.get('href') == 'javascript:':
                    continue
                post_num_temp = subpost.get('id')            
                post_author_temp = subpost.find(class_ = 'username').get_text()
                post_seq_temp = subpost.find(class_ = 'publicControls').find(class_='postNumber').get_text()
                
    
                # Get post date, there are two scenarios or more, if the tag abbr exist, then it is possible to get unix
                # time string, if the tag span exists, then it is possible to get the time by getting the text of the tag
                if type(subpost.find(class_='datePermalink').find('abbr')) != type(None):
                    subpost_nix_time = subpost.find(class_='datePermalink').abbr.get('data-time')
                elif type(subpost.find(class_='datePermalink').find('span')) != type(None):
                    subpost_str_time = subpost.find(class_='datePermalink').span.get('title').encode('utf-8')
                    subpost_time= subpost_str_time[:10] + ' ' + subpost_str_time[-5:]
                    subpost_strptime = time.strptime(subpost_time,'%Y-%m-%d %H:%M')
                    subpost_nix_time = time.mktime(subpost_strptime)
                subpost_unix_time_temp = subpost_nix_time
                
                #Find contents here
                #Find if a subpost have likes, how many, and who liked it.
                like_tag = subpost.find(id = 'likes-' + post_num_temp)
                if type(like_tag.a) == type(None):
                    like_tag_exist = 'N'
                elif type(like_tag.a) != type(None):
                    like_tag_exist = 'Y'
                like_tag_list_temp = like_tag_exist
    
                if like_tag_exist == 'N':
                    like_tag_nums_temp = 0
                    like_tag_users_temp = 'None'
                
                elif like_tag_exist == 'Y':
                    likes_page = 'https://www.bbsapp.org/posts/' + post_num_temp[5:] + '/likes'
                    eg_likes_page = s.get(likes_page)
                    soup_likes_page = BeautifulSoup(eg_likes_page.text, 'html.parser')
                    if type(soup_likes_page.find(class_='overlayScroll')) != type(None):
                        like_user_tablist = soup_likes_page.find(class_='overlayScroll').find_all(name = 'h3', class_='username')
                        like_tag_nums_temp = len(like_user_tablist)
                        like_users = []
                        for sub_like_tabuser in like_user_tablist:
                            like_user_single = sub_like_tabuser.get_text()
                            like_users.append(like_user_single)
    
                    elif type(soup_likes_page.find(class_='overlayScroll')) == type(None):
                        like_user_tablist = subpost.find(id = 'likes-' + post_num_temp).find('span').find_all('a')
                        like_tag_nums_temp = len(like_user_tablist)
                        like_users = []
                        for sub_like_tabuser in like_user_tablist:
                            like_user_single = sub_like_tabuser.get_text()
                            like_users.append(like_user_single)
    
                    like_tag_users_temp  = ';'.join(like_users)
                
                
                #Find quotes here
                quoted_author_sublist = []
                quoted_thread_id = []
                quote_tags = subpost.find(name = 'blockquote').find_all('div', class_ = re.compile('bbCodeQuote'))
                if type(subpost.find(name = 'blockquote').find('div', class_ = re.compile('bbCodeQuote'))) == type(None):
                    quoted_author_sublist.append('Not_A_User')
                    quoted_thread_id.append(np.nan)    
                quote_numbers_temp = len(quote_tags)
    
                for quote_tags_single in quote_tags:
                    if type(quote_tags_single.get('data-author')) != type(None) and type(quote_tags_single.find(class_='AttributionLink')) != type(None): 
                        if type(quote_tags_single.find(class_='AttributionLink').get('href')) != type(None):
                            quoted_author_sublist.append(quote_tags_single.get('data-author'))
                            #New stuff here, I got the thread id that was quoted, it can be used to calculate H-index etc.
                            hash_tag_pos = quote_tags_single.find(class_='AttributionLink').get('href').index('#')+1
                            quoted_thread_id.append(quote_tags_single.find(class_='AttributionLink').get('href')[hash_tag_pos:])
                        else:
                            quoted_author_sublist.append('Not_A_User')
                            quoted_thread_id.append(np.nan)
                    else:
                        quoted_author_sublist.append('Not_A_User')
                        quoted_thread_id.append(np.nan)
                    
    
    
                #Changes needed here: change how the append is done, asign individual temp names for the appended items, append in the end/
                #Some temps needed to be multiplied by len(quoted_author_single)
                
                #Bug fix here, if a value is going to be missing, assign missing to it.
                
                #Calculate the multiplier, if only one quoted or nobody quoted, the it is 1, other wise, it is len(quoted_author_sublist)
                if len(quoted_author_sublist) <= 1:
                    multiplier = 1
                else:
                    multiplier = len(quoted_author_sublist)
                
                post_author = post_author + [post_author_temp] * multiplier
                post_num = post_num + [post_num_temp] * multiplier
                subpost_unix_time = subpost_unix_time+ [subpost_unix_time_temp] * multiplier
                post_seq = post_seq + [post_seq_temp] * multiplier
                like_tag_list = like_tag_list+ [like_tag_list_temp] * multiplier
                like_tag_nums = like_tag_nums+ [like_tag_nums_temp] * multiplier
                like_tag_users = like_tag_users + [like_tag_users_temp] * multiplier
                quote_numbers = quote_numbers + [quote_numbers_temp] * multiplier
                quoted_author = quoted_author + quoted_author_sublist
                quoted_thread = quoted_thread + quoted_thread_id
                print multiplier
                
                # Post body: Only get none quote, text string  contents.
                # Two conditions, with quote or without quote.
                #pbody_raw = list(subpost.article.blockquote.find_all(recursive=False)) & (child.attrs['class'] != [u'bbCodeBlock', u'bbCodeQuote'])
                #pbody_temp = []
                #for child in subpost.article.blockquote.find_all(recursive=False):
                #    if (type(child) == bs4.element.NavigableString) & (child.encode('utf8') != ('\n')):
                #        pbody_temp.append(child.encode('utf8'))
                #    if (type(child) == bs4.element.Tag) & (len(child) != 0):
                #        if (child.attrs['class'] != [u'messageTextEndMarker']) :
                #            pbody_temp.append(child.getText().encode('utf8'))
                #post_body.append(';'.join(pbody_temp))
                pbody_raw = subpost.article.blockquote.getText().encode('utf8')
                op_start_phrase = '\n\t\t\t\t\t\n\t\t\t\t\t'
                op_end_phrase = '\n\xc2\xa0\n'
                reply_end_phrase = '\n\t\t\t\t\t\xc2\xa0\n'
                op_start = pbody_raw.find(op_start_phrase)
                op_end_ = pbody_raw.find(op_end_phrase)
                reply_end = pbody_raw.find(reply_end_phrase)
                
                end_quote = pbody_raw.find('\x95\xe5\xbc\x80...')
                
                
                if end_quote == -1:
                    if (op_start != -1) & (op_end_ != -1):
                        post_body.extend([pbody_raw[op_start+12:op_end_]]  for i in range(multiplier))
                    else:
                        post_body.extend([pbody_raw]  for i in range(multiplier))
                else:
                    post_body.extend([pbody_raw[end_quote+9:reply_end].decode('utf8').encode('utf8')] for i in range(multiplier))
    
        # Now summerize the info for the entire post. The summerized dataframe will have 9 columns we iterated to get, and also one more column that will be used to concat with title_pd
        sub_thread_link = [sublink] * len(post_author)
        print len(sub_thread_link)
        sub_posts_pd = pd.DataFrame({'thread_link':sub_thread_link, 'post_author':post_author, 'post_num':post_num, 'subpost_unix_time':subpost_unix_time, 'post_seq':post_seq, 'like_tag_list':like_tag_list, 'like_tag_nums':like_tag_nums, 'like_tag_users':like_tag_users, 'quote_numbers':quote_numbers, 'quoted_author':quoted_author,'quoted_thread':quoted_thread ,'post_body':post_body})
        
        # Subset from title_pd the row that full subpost info is being extracted
        title_row_num = thread_link_list_master.index(sublink)
        title_subset_df = title_pd.iloc[[title_row_num],:]
        title_subset_df.reset_index(inplace = True)
        subpost_sum_pd = pd.merge(title_subset_df, sub_posts_pd, how='inner', on=['thread_link'])
        subpost_thread_id = subpost_sum_pd['thread_id'].iloc[0]
        # subpost file save link:
        subpost_csv_path = os.path.normpath(subpost_temp_path +  subpost_thread_id + "_subpost.csv")
        subpost_sum_pd.to_csv(subpost_csv_path,encoding='utf-8')
        
        return sublink
        
    start = time.time()
    results = ThreadPool(5).imap_unordered(subpost_spider, thread_link_list)
    for sublink in results:
        print("%r fetched in %ss" % (sublink, time.time() - start))
    
    print("Elapsed Time: %s" % (time.time() - start,))
    
if __name__ == "missing_file_repeat":
    missing_file_repeat()