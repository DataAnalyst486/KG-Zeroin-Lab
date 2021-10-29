#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
import datetime
import pymssql
import math
import time
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings(action='ignore')


# In[600]:


class kospi200_sentiment_keyword:
    
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
    
    def corp_daily_senti_keyword(self, df):
        
        df.TITLE = df.TITLE.str.replace('대상','')
        df.final_title = df.final_title.str.replace('대상','')
        
        final_keyword = pd.DataFrame(columns = ['DATE', 'NAME', 'KEYWORD', 'LABEL', 'SCORE'])        
        
        def text_split(text):
            text = text.split()
            return text

        def tfidf_count(text, max_features, n_gram = 2):
            tfidf = TfidfVectorizer(max_features=max_features, tokenizer=text_split,sublinear_tf=True, ngram_range = (n_gram,n_gram))
            tdm = tfidf.fit_transform(text)
            word_count_tfidf = pd.DataFrame({
                '단어': tfidf.get_feature_names(),
                '빈도': tdm.sum(axis=0).flat
            })
            return word_count_tfidf
        
        def match_word(word):
            matching = [s for s in corp_news[corp_news['final_title'].str.contains(word)]['TITLE'].str.split().values[0] if word in s][0]
            matching = re.sub('[=+,#-/”“▶▲●◆■◀\?.:^$’@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》☞△&·‥""]', ' ', matching).split()
            matching = [s for s in matching if word in s][0]
            return matching
    
        for comp in tqdm(self.kospi200['종목명']):

            corp_news = df[df['final_title'].str.contains(comp)].reset_index(drop=True)
            corp_news.TITLE = corp_news.TITLE.str.replace('㈜', '')
            corp_news.final_title = corp_news.final_title.str.upper()
            corp_news.TITLE = corp_news.TITLE.str.upper()

            if len(corp_news) > 2:
                try:
                    negative_tfidf = tfidf_count(corp_news[corp_news.news_sentiment == -1]['final_title'], 15,1).sort_values('빈도', ascending=False).reset_index()
                    neg_mm = MinMaxScaler()
                    negative_tfidf['빈도'] = neg_mm.fit_transform(negative_tfidf[['빈도']])
                    negative_tfidf['빈도'] = np.where(negative_tfidf['빈도'] > 0.95, negative_tfidf['빈도']- 0.05 , negative_tfidf['빈도'] + 0.05)
                    negative_tfidf.단어 = negative_tfidf.단어.apply(lambda x : x.upper())
                    negative_tfidf.단어 = negative_tfidf.단어.apply(lambda x : match_word(x))
                    negative_tfidf = negative_tfidf[~(negative_tfidf['단어'] == comp)]
                    negative_tfidf = negative_tfidf.drop_duplicates('단어').reset_index(drop=True)
                    negative_tfidf['LABEL'] = -1
                except:
                    negative_tfidf = pd.DataFrame(columns = ['단어','빈도','LABEL'])

                try:
                    positive_tfidf = tfidf_count(corp_news[corp_news.news_sentiment == 1]['final_title'], 15,1).sort_values('빈도', ascending=False).reset_index()
                    pos_mm = MinMaxScaler()
                    positive_tfidf['빈도'] = pos_mm.fit_transform(positive_tfidf[['빈도']])
                    positive_tfidf['빈도'] = np.where(positive_tfidf['빈도'] > 0.95, positive_tfidf['빈도'] - 0.05 , positive_tfidf['빈도'] + 0.05)
                    positive_tfidf.단어 = positive_tfidf.단어.apply(lambda x : x.upper())
                    positive_tfidf.단어 = positive_tfidf.단어.apply(lambda x : match_word(x))
                    positive_tfidf = positive_tfidf[~(positive_tfidf['단어'] == comp)]
                    positive_tfidf = positive_tfidf.drop_duplicates('단어').reset_index(drop=True)
                    positive_tfidf['LABEL'] = 1
                except:
                    positive_tfidf = pd.DataFrame(columns = ['단어','빈도','LABEL'])

                neut_num = (10-len(negative_tfidf)-len(positive_tfidf))
                neut_tfidf = pd.DataFrame(columns = ['단어','빈도','LABEL'])

                if neut_num > 0:
                    try:
                        neut_tfidf = tfidf_count(corp_news[corp_news.news_sentiment == 0]['final_title'], neut_num+1,1).sort_values('빈도', ascending=False).reset_index()
                        neut_mm = MinMaxScaler()
                        neut_tfidf['빈도'] = neut_mm.fit_transform(neut_tfidf[['빈도']])
                        neut_tfidf['빈도'] = np.where(neut_tfidf['빈도'] > 0.95, neut_tfidf['빈도'] - 0.05 , neut_tfidf['빈도'] + 0.05)
                        neut_tfidf.단어 = neut_tfidf.단어.apply(lambda x : x.upper())
                        neut_tfidf.단어 = neut_tfidf.단어.apply(lambda x : match_word(x))
                        neut_tfidf = neut_tfidf[~(neut_tfidf['단어'] == comp)]
                        neut_tfidf = neut_tfidf.drop_duplicates('단어').reset_index(drop=True).iloc[:neut_num]
                        neut_tfidf['LABEL'] = 0
                    except:
                        pass
            
                negative_tfidf = negative_tfidf.iloc[:5]
                positive_tfidf = positive_tfidf.iloc[:5]
                neut_tfidf = neut_tfidf.iloc[:5,1:]
                
                
                comp_final = pd.concat([positive_tfidf, negative_tfidf, neut_tfidf]).reset_index(drop=True)
                comp_final = comp_final.drop_duplicates('단어').reset_index(drop=True)
                comp_final['NAME'] = comp
                comp_final['DATE'] = self.yesterday
                comp_final.rename(columns={'단어':'KEYWORD','빈도':'SCORE'}, inplace=True)
                comp_final = comp_final[['DATE', 'NAME', 'KEYWORD', 'LABEL', 'SCORE']]

                final_keyword = final_keyword.append(comp_final)
                
        final_keyword = final_keyword.reset_index(drop=True)
        
        return final_keyword


# In[601]:


s = kospi200_sentiment_keyword()


# In[602]:


df, label_mm = s.news_load()


# In[603]:


df


# In[604]:


corp_daily_senti_keyword = s.corp_daily_senti_keyword(df)
corp_daily_senti_keyword


# In[605]:


conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()
for i in corp_daily_senti_keyword.values:
    try:
        insert_query = "INSERT INTO CORP_SENTIMENT_KEYWORD (DATE, NAME, KEYWORD, LABEL, SCORE) VALUES (%s, %s, %s, %s, %s) "
        sql_data = tuple(i)
        cursor.execute(insert_query, sql_data)
        conn.commit()
    except:
        pass
    
conn.close()


# In[575]:


# conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
# cursor = conn.cursor()

# sql = 'CREATE TABLE CORP_SENTIMENT_KEYWORD(DATE DATE, NAME varchar(32), KEYWORD varchar(32), LABEL int, SCORE float, CONSTRAINT PK_CORP_SENTIMENT_KEYWORD PRIMARY KEY (DATE,NAME,KEYWORD))'

# cursor.execute(sql)

# conn.commit()
# conn.close()

