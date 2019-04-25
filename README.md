# website_text_analysis

This repositary is going to gather data from a public forum and do text analysis of data gathered. It is consisted with the following sections

(1) Data extraction and update. 

It will log in to website and crawl/parse infomation needed and update data to master datafile. Schedule it on crontab and update daily.

This function is completed by bbsapp-update-nas.py and its dependency exception_handler.py and subpost_spider.py

(2) Data analysis for user social network graphs.

It uses

(3) Word cloud generator

Applying RNN based word-to-vector (pre-trained) to break down setences to words. Use the results to find trendy topics and generate wordcloud visulization

(4) Unsupervised learning to cluster user by timezone from activity pattern

Use k-means algorithm to cluster user by their activity pattern, the resulting groups can be interpreted as approximately their timezone.
