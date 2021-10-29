#!/usr/bin/env python
# coding: utf-8

# # 전처리

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
# import matplotlib.pyplot as plt
# plt.rc('font', family='NanumBarunGothic')

warnings.filterwarnings(action='ignore')


# In[205]:


today = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
#today = '2021-10-10'
print(today)


# In[207]:


## 테이블 생성
conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

sql = f"SELECT * FROM NEWS_LIST_NAVER WHERE NEWS_DATE BETWEEN '{today} 00:00:00' AND '{today} 23:59:59.997'"

cursor.execute(sql)

rows = cursor.fetchall()
conn.commit()
conn.close()


# In[208]:


col = ['IDX', 'CATEGORY','NEWS_DATE', 'TITLE', 'clean_content',
       'URL_DETAIL', 'final_title', 'final_content',
       'positive_score_mean', 'negative_score_mean', 'positive_score_sum',
       'negative_score_sum', 'news_sentiment','news_label','good','bad','source']
df=pd.DataFrame(np.array(rows), columns=col)


# In[209]:


# 인코딩

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


# In[217]:

df = df[df['clean_content'] != 'blank']
df.fillna('', inplace=True)
df.reset_index(inplace=True)
df = df.iloc[:,1:]
df['IDX'] = df['IDX'].astype(str)


# In[120]:


news_week = df.copy()


# # 모델링

# In[121]:


from gensim.models import FastText
import time

start = time.time()

model = FastText(sentences = news_week['final_content'].str.split(), size  = 100, window = 4, min_count = 5, workers = 4, sg = 1, iter=50, word_ngrams=5)

end = time.time()

print('{} 분'.format((end - start)/60))


# In[122]:


model.save(f'C:/Users/남건우/Desktop/batch/data/model/FastText_{today}.model')


# In[123]:


word_label = pd.read_csv('C:/Users/남건우/Desktop/batch/data/word_label.csv', encoding='cp949').iloc[:,:2]
# word_label = word_label[word_label.label != 0.5]
# word_label = word_label.replace(0,-1)


# In[124]:


news_sum = []

for i in tqdm(range(len(news_week))):
    temp = str(news_week['final_content'][i]).split()
    news_sum = []
#     temp_word = []
    for j in range(len(temp)):
        one_word = []
        
        for k in range(len(word_label)):
            try:
                one_word.append(model.wv.similarity(temp[j],word_label['word'][k]))
                
            except:
                one_word.append(0)
        
        one_word_label = sorted(np.multiply(one_word,word_label['label']), key=abs, reverse=True)
        
        try:
            word_sum = sum([x for x in one_word_label if np.abs(x) >= 0.5][:10])
            
        except:
            word_sum = 0
        
#         temp_word.append([temp[j],word_sum])
                                 
        news_sum.append(word_sum)
    try:    
        pos = [news_sum[i] for i in range(len(news_sum)) if news_sum[i] > 0]
        neg = [news_sum[i] for i in range(len(news_sum)) if news_sum[i] < 0]

        news_week['news_label'][i] = float(np.round(np.mean(news_sum), decimals=2))
        news_week['positive_score_mean'][i] = float(np.round(np.mean(pos), decimals=2))
        news_week['negative_score_mean'][i] = float(np.round(np.mean(neg), decimals=2))
        news_week['positive_score_sum'][i] = float(np.round(np.sum(pos), decimals=2))
        news_week['negative_score_sum'][i] = float(np.round(np.sum(neg), decimals=2))
        
    except:
        news_week['news_label'][i] = 0
        news_week['positive_score_mean'][i] = 0
        news_week['negative_score_mean'][i] = 0
        news_week['positive_score_sum'][i] = 0
        news_week['negative_score_sum'][i] = 0


# In[125]:


news_week['news_sentiment'] = np.where(news_week.news_label >= 0.15, 1, np.where(news_week.news_label <= -0.05,-1,0))


# In[126]:


news_week.to_csv(f'C:/Users/남건우/Desktop/batch/data/news/final_news{today}.csv', index=False)


# In[127]:


f = pd.read_csv(f'C:/Users/남건우/Desktop/batch/data/news/final_news{today}.csv')
f['IDX'] = f['IDX'].astype(str)
f.fillna('', inplace=True)


# # 데이터 삽입

# In[8]:


conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

insert_query = """
   UPDATE NEWS_LIST_NAVER
   SET positive_score_mean=%s, negative_score_mean=%s, positive_score_sum=%s, negative_score_sum=%s, news_sentiment=%s, news_label=%s
   WHERE IDX=%s and CATEGORY =%s
"""


# In[5]:


points = math.ceil(len(f) / 1000)
points


# In[9]:


for i in tqdm(range(points)):
    df = [tuple(x) for x in f[['positive_score_mean','negative_score_mean','positive_score_sum','negative_score_sum','news_sentiment','news_label','IDX','CATEGORY']][i*1000:(i+1)*1000].values]
    cursor.executemany(insert_query,df)
    time.sleep(20)

conn.commit()
conn.close()


# In[ ]:




