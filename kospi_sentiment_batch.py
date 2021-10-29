#!/usr/bin/env python
# coding: utf-8

# In[276]:


# import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
import datetime
import pymssql
import math
import time
import re

class kospi200_API:
    
    def __init__(self):
        self.startday = str(datetime.datetime.now() - datetime.timedelta(days=3))[:10]
        self.yesterday = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]

#         self.startday = startday
#         self.yesterday = yesterday
        self.kospi200 = pd.read_csv('C:/Users/남건우/Desktop/batch/data/kospi_200.csv')

        for i in range(200):
            p = (6 - len(str(self.kospi200['종목코드'][i]))) * str(0)
            self.kospi200.loc[i,'종목코드'] = p+str(self.kospi200['종목코드'][i])
            
        print(self.startday, self.yesterday)

    def news_load(self):

        ## 테이블 생성
        conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
        cursor = conn.cursor()

        sql = f"SELECT * FROM NEWS_LIST_NAVER WHERE NEWS_DATE BETWEEN '{self.startday} 00:00:00' AND '{self.yesterday} 23:59:59.997'"

        cursor.execute(sql)

        rows = cursor.fetchall()
        conn.commit()
        conn.close()

        col = ['IDX', 'CATEGORY','NEWS_DATE', 'TITLE', 'clean_content',
               'URL_DETAIL', 'final_title', 'final_content',
               'positive_score_mean', 'negative_score_mean', 'positive_score_sum',
               'negative_score_sum', 'news_sentiment','news_label','good','bad','source']

        df=pd.DataFrame(np.array(rows), columns=col)

        title = []
        content = []
        source = []
        final_title = []
        final_content = []

        error = []

        for i in range(len(df)):
            try:
                title.append(df.iloc[i, 3].encode('iso-8859-1').decode('euc-kr'))
            except:
                title.append(0)
                error.append(i)

        for i in range(len(df)):
            try:
                content.append(df.iloc[i, 4].encode('iso-8859-1').decode('euc-kr'))
            except:
                content.append(0)
                error.append(i)

        for i in range(len(df)):
            try:
                source.append(df.iloc[i, 1].encode('iso-8859-1').decode('euc-kr'))
            except:
                source.append(0)
                error.append(i)

        for i in range(len(df)):
            try:
                final_title.append(df.iloc[i, 6].encode('iso-8859-1').decode('euc-kr'))
            except:
                final_title.append(0)
                error.append(i)

        for i in range(len(df)):
            try:
                final_content.append(df.iloc[i, 7].encode('iso-8859-1').decode('euc-kr'))
            except:
                final_content.append(0)
                error.append(i)

        len(title), len(content), len(source), len(df)
        error = list(set(error))
        df['TITLE'] = title
        df['clean_content'] = content
        df['CATEGORY'] = source
        df['final_title'] = final_title
        df['final_content'] = final_content

        df = df.drop(error).reset_index(drop=True)
        df = df.sort_values('NEWS_DATE',ascending=False).reset_index(drop=True)
        df = df[df['clean_content'] != 'blank']

        from sklearn.preprocessing import MinMaxScaler

#         pos_mm = MinMaxScaler()
#         neg_mm = MinMaxScaler()
        label_mm = MinMaxScaler()


#         df['sc_positive_score_mean'] = pos_mm.fit_transform(df[['positive_score_mean']]) * 100
#         df['sc_negative_score_mean'] = neg_mm.fit_transform(abs(df[['negative_score_mean']])) * 100
        
        label_mm.fit(df[['news_label']])
        df['sc_news_label_score'] = label_mm.transform(df[['news_label']]) * 100
        
        
        return df, label_mm


    def corp_daily_senti(self, df):

#         final_corp = pd.DataFrame(columns = ['date', 'name', 'positive','negative', 'total_score'])
        final_corp = pd.DataFrame(columns = ['date', 'name', 'sentiment'])

        for i in tqdm(self.kospi200['종목명']):
            
            corp_news = df[df['final_title'].str.contains(i)].reset_index(drop=True)
            
            if len(corp_news) > 2:

                ls = []

#                 ls.append([self.yesterday, i, corp_news['sc_positive_score_mean'].mean() , corp_news['sc_negative_score_mean'].mean(), corp_news['sc_news_label_score'].mean()])
                ls.append([self.yesterday, i, corp_news['sc_news_label_score'].mean()])

#                 corp_df = pd.DataFrame(ls, columns = ['date', 'name', 'positive','negative','total_score'])
                corp_df = pd.DataFrame(ls, columns = ['date', 'name', 'sentiment'])


                final_corp = final_corp.append(corp_df)

        final_corp = final_corp.reset_index(drop=True)
        final_corp.dropna(inplace=True)

        return final_corp

    def corp_daily_news(self, df, corp_list):
         
        final_news = pd.DataFrame(columns = ['date','name','IDX','TITLE','final_title','clean_content','URL_DETAIL','news_sentiment','source'])

        for i in tqdm(corp_list):
            
            corp_neg_news = df[(df['final_title'].str.contains(i)) & (df['news_sentiment'] == -1)].reset_index(drop=True).sort_values(['NEWS_DATE','sc_news_label_score'], ascending=True)
            corp_pos_news = df[(df['final_title'].str.contains(i)) & (df['news_sentiment'] == 1)].reset_index(drop=True).sort_values(['NEWS_DATE','sc_news_label_score'], ascending=False)

            edaily_pos = corp_pos_news[corp_pos_news['URL_DETAIL'].str.contains('www.edaily.co.kr')]
            edaily_neg = corp_neg_news[corp_neg_news['URL_DETAIL'].str.contains('www.edaily.co.kr')]

            corp_pos_news = corp_pos_news[~(corp_pos_news['URL_DETAIL'].str.contains('www.edaily.co.kr'))]
            corp_neg_news = corp_neg_news[~(corp_neg_news['URL_DETAIL'].str.contains('www.edaily.co.kr'))]

            corp_pos_news['URL_DETAIL'] = None
            corp_neg_news['URL_DETAIL'] = None

            final_pos = pd.concat([edaily_pos, corp_pos_news[:3]])
            final_neg = pd.concat([edaily_neg, corp_neg_news[:3]])

            corp_news = pd.concat([final_pos, final_neg]).reset_index(drop=True)

            corp_news['date'] = self.yesterday
            corp_news['name'] = i
            corp_news = corp_news[['date','name','IDX','TITLE','final_title','clean_content','URL_DETAIL','news_sentiment','source']]

            final_news = final_news.append(corp_news)

        final_news.drop_duplicates('final_title', inplace=True)
        final_news = final_news.reset_index(drop=True)

        return final_news


# In[306]:


s = kospi200_API()


# In[307]:


df, label_mm = s.news_load()


# In[267]:


corp_daily_senti = s.corp_daily_senti(df)
corp_daily_senti['label'] = np.where(corp_daily_senti.sentiment >= (label_mm.transform(np.array(0.15).reshape(1,-1)) *100)[0][0], 1, np.where(corp_daily_senti.sentiment <= (label_mm.transform(np.array(-0.05).reshape(1,-1)) *100)[0][0],-1,0))
corp_daily_senti

# In[308]:


corp_daily_news = s.corp_daily_news(df,corp_daily_senti['name'])

# ls = []
# for idx in range(len(corp_daily_news)):
#     t = []
#     for word in corp_daily_news['final_title'][idx].split():
#         try:
#             matching = [s for s in corp_daily_news['TITLE'][idx].split() if word in s][0]
#             matching = re.sub('[=+,#-/”“▶▲●◆■◀\?.:^$’@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》☞△&·‥""]', ' ', matching).split()
#             matching = [s for s in matching if word in s][0]
#             if matching not in t:
#                 t.append(matching)
#         except:
#             pass
#     ls.append(t)
#
# # corp_daily_news['new_title'] = ''
# for i in range(len(corp_daily_news)):
#     corp_daily_news['final_title'][i] = ' '.join(ls[i])

for i in range(len(corp_daily_news)):
    s = corp_daily_news['source'][i]
    t = corp_daily_news['TITLE'][i]
    corp_daily_news['final_title'][i] = f'[{s}] {t}'

corp_daily_news = corp_daily_news.drop(columns='source')
corp_daily_news = corp_daily_news.reset_index(drop=True)


# In[250]:


conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

for i in corp_daily_senti.values:
    try:
        insert_query ="INSERT INTO CORP_SENTIMENT (DATE, NAME, SENTIMENT, LABEL) VALUES (%s, %s, %s, %s)"
        sql_data = tuple(i)
        cursor.execute(insert_query, sql_data)
        conn.commit()
    except:
        pass

conn.close()


# In[309]:


conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

for i in corp_daily_news.values:
    try:
        insert_query = "INSERT INTO CORP_SENTIMENT_NEWS (DATE, NAME, IDX, TITLE, FINAL_TITLE, FINAL_CONTENT, URL_DETAIL, LABEL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        sql_data = tuple(i)
        cursor.execute(insert_query, sql_data)
        conn.commit()
    except:
        pass

conn.close()


# In[ ]:





# In[272]:

#
# conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
# cursor = conn.cursor()
#
# sql = 'CREATE TABLE CORP_SENTIMENT_NEWS(DATE DATE, NAME varchar(32), IDX varchar(32), TITLE TEXT, FINAL_TITLE TEXT, FINAL_CONTENT TEXT, URL_DETAIL TEXT, LABEL int, CONSTRAINT PK_CORP_SENTIMENT_NEWS PRIMARY KEY (DATE,NAME,IDX))'
#
# cursor.execute(sql)
#
# conn.commit()
# conn.close()
#
#
#
#
#
# conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
# cursor = conn.cursor()
#
# sql = 'CREATE TABLE CORP_SENTIMENT(DATE DATE, NAME varchar(32), SENTIMENT float, LABEL int)'
#
# cursor.execute(sql)
#
# conn.commit()
# conn.close()

