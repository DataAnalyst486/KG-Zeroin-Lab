#!/usr/bin/env python
# coding: utf-8

# # ESG

# In[4]:



# In[6]:


import requests
import sys
import pandas as pd
import numpy as np
import pymssql
import re
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interacrivity="all"
from tqdm.notebook import tqdm
import datetime
from datetime import timedelta
import warnings
warnings.filterwarnings(action='ignore')

now = datetime.datetime.now() - timedelta(7)
date = now.strftime('%Y-%m-%d')

next_week = (now+datetime.timedelta(6)).strftime('%Y-%m-%d')

print(date, next_week)

## 테이블 생성
conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

sql = f"SELECT * FROM NEWS_LIST_NAVER WHERE NEWS_DATE BETWEEN '{date} 00:00:00' AND '{next_week} 23:59:59.997'"

cursor.execute(sql)

rows = cursor.fetchall()
conn.commit()
conn.close()

print('----------------------뉴스 불러오기----------------------')
col = ['IDX', 'CATEGORY','NEWS_DATE', 'TITLE', 'clean_content',
       'URL_DETAIL', 'final_title', 'final_content',
       'positive_score_mean', 'negative_score_mean', 'positive_score_sum',
       'negative_score_sum', 'news_sentiment','news_label','good','bad','source']
df=pd.DataFrame(np.array(rows), columns=col)


# 인코딩
   
title = []
content = []
source = []
final_title = []
final_content = []

error = []

for i in range(len(df)):
    try:
        title.append(df.iloc[i,3].encode('iso-8859-1').decode('euc-kr'))
    except:
        title.append(0)
        error.append(i)

for i in range(len(df)):
    try:
        content.append(df.iloc[i,4].encode('iso-8859-1').decode('euc-kr'))
    except:
        content.append(0)
        error.append(i)
        
for i in range(len(df)):
    try:
        source.append(df.iloc[i,1].encode('iso-8859-1').decode('euc-kr'))
    except:
        source.append(0)
        error.append(i)
        
for i in range(len(df)):
    try:
        final_title.append(df.iloc[i,6].encode('iso-8859-1').decode('euc-kr'))
    except:
        final_title.append(0)
        error.append(i)  
        
for i in range(len(df)):
    try:
        final_content.append(df.iloc[i,7].encode('iso-8859-1').decode('euc-kr'))
    except:
        final_content.append(0)
        error.append(i)  

len(title) , len(content), len(source) , len(df)
error = list(set(error))
df['TITLE'] = title
df['clean_content'] = content
df['CATEGORY'] = source
df['final_title'] = final_title
df['final_content'] = final_content

df = df.drop(error).reset_index(drop=True)

esg_news = df[(df.final_content.str.contains('ESG')) | (df.final_content.str.contains('탄소')) | (df.final_content.str.contains('환경'))].sort_values('NEWS_DATE',ascending=False).reset_index(drop=True)

esg_news['final_title'] = esg_news['final_title'].apply(
    lambda x: x.replace(' 너지','에너지'))

stopword = [
    '아직도','추천주','무단전재','재배포','금지','및','돈내고','받으세요','클릭' , '이젠', '카톡방','받는다','아직','무료','한국경제',
    '아시아경제','Global Economy','이데일리','매일경제','연합뉴스','정보통신신문','아이투자','헬스코리아','뉴스와이어','델톤','.com','아이투자','저작권자 ',
    'www.itooza.com','무단전재','재배포','금지','한국경제','무료','카톡방','추천주','추천','클릭','번호','하루만에','신청즉시','관심종목','뉴스','대장주',
    '즉시참여하기','신청폭주상','빠른신청은','카운트 다운','종목명','계좌','한국경제신문','금지','기자','hankyung.com','아시아경제','ioy asiae.co.kr','quot',
    '받으세요','광고','뉴스카페','ⓒ','ace','매경이코노미','매일경제','무단전재 및 재배포 금지 ','mk.co.kr','\\n☞','asiae.co.kr','종합 경제정보','뉴스 앱',
    '속보','주목하세요','적중','터질','종목','주목','카카오톡','드립니다','기사','관련','무조건','대박','비율','연','월','일','훨훨','따라오','체험','선착순',
    '특급','터질','오늘','초보','기업','확인','증권가','무조건','연도','명이','지금','당장','이것','사라','주린','리딩','평생','바로','입장','남발','여의도',
    '궁금','체험','한가','폭탄','한경','닷컴','정보','투자','방송','전문가','종합','주식','역대','분기','금융','신청','실시간','당신','미디어','합니다',
    '시험','탁론','모바일','플러스','본드','마켓','언제','트래블러','어디','생활','신문','포인트','위한','분야','기반','통해','위해','한다','그룹',
    '브랜드','스타','된다','위원회','해야','경우','대한','때문','국민','따르','연구원','수수료','대출','없이','증권사','신용','저작권자','컨텐츠',
    '최신','입니다','즉시','문자','발송','내일','상담','링크','고민','타이밍','여러분','이제','모르','마감','간편','한국','즐거움','온라인','기관','대비',
    '알고리즘','그래프','최근','작성','해당','관심','근거','참고','포착','대해','인공','마지막','최근','코리아','스타일','보도','직원','황제','탄생','행진',
    '트렌드','진단','이날','전년','부문','대표','비실비실대다','i레터','불리우는','대국민','비실비실대','레터','옴디아', '무단','전재','배포','관련주','련주','카드뉴스',
    '피쉬','칩스','신기록(종합2보)','연합뉴스','에프앤가이드','MARKETPOINT','명예','전당','위험','등급','가격','신탁','증권','목적','신탁',
    '수익','자산','채권','위험','지수','증권','집합','추구','목적','전략','운용','등급','변동','달성','보장','상품','업자','이상','시장','대상','재산','국내','기준',
    '파생','기구','펀드','수준','비교','가격','주가','손실','회사','평가','기초','상환','안정','가치','장외','포트폴리오','가능','조기','해외','분석','비중','구성','변경',
    'TIGER', 'KODEX', 'KINDEX', 'KBSTAR', 'TIMEFOLIO', 'ARIRANG', 'HANARO', '흥국', 'SMART', 'TREX', '마이티', '파워', '마이다스', 'FOCUS', 'KOSEF','FnGuide','산출',
     '상장','제공', '최대', '거래소', '거래','상장지수','주요','대형','매도','계획','편입','제외','수단','실행', '연동','조정','투자자','시간','and','보고서', '현지',
    '고객','공개','최소한','거래일','제시','판단','시점','증시','경제','이후','금액','발표','국가','이유','시가총액','기간','미만','전체','규모','시작','전날',
    '자료','조사','관리','필요','영업','일반','다양','신규','이벤트','가운데','목표','가입','전망','지급','차지','집중','실적','예상','기대','고려','vs','사업','펀드','활용',
    '확대','추진','영업','업무','발행','국내','구축','카드'
]


from eunjeon import Mecab
mecab = Mecab('C:/mecab/mecab-ko-dic')

# tag_classes = ['NNG', 'NNP','VA', 'VV+EC', 'XSV+EP', 'XSV+EF', 'XSV+EC', 'VV+ETM', 'MAG', 'MAJ', 'NP', 'NNBC', 'IC', 'XR', 'VA+EC','SL']
# stopword = ['TIGER', 'KODEX', 'KINDEX', 'KBSTAR', 'TIMEFOLIO', 'ARIRANG', 'HANARO', '흥국', 'SMART', 'TREX', '마이티', '파워', '마이다스', 'FOCUS', 'KOSEF']
def extract_noun_mecab_etf(text):     # 형태소중 '조사'를 제외하고 clean_word에 추가.
    clean_word = []
    result = mecab.pos(text)
    for word, tag in result:
        if tag.startswith('N') or tag.startswith('SL'):
            if (word not in stopword) and len(word) > 1:
                clean_word.append(word)
            
    return clean_word   

from sklearn.feature_extraction.text import TfidfVectorizer

def tfidf_count(text, max_features):
    tfidf = TfidfVectorizer(max_features=max_features, tokenizer=extract_noun_mecab_etf,sublinear_tf=True)
    tdm = tfidf.fit_transform(text)
    word_count_tfidf = pd.DataFrame({
    '단어':tfidf.get_feature_names(),
    '빈도':tdm.sum(axis=0).flat
    })
    return word_count_tfidf
print('----------------------keyword 추출----------------------')
word = tfidf_count(esg_news['final_title'], 200).sort_values('빈도', ascending=False).reset_index(drop=True)

from sklearn.preprocessing import MinMaxScaler

mm = MinMaxScaler()
word['빈도'] = (mm.fit_transform(word[['빈도']])+0.05) * 100

ls = list(pd.read_csv('C:/Users/남건우/Desktop/batch/data/esg_keyword.csv', encoding='cp949')['단어'])
word = word[(word['단어'].isin(ls)) & (word['단어'] != 'esg')].reset_index(drop=True)

## 관련 ETF 상품 가져오기

etf_pre = pd.read_csv('C:/Users/남건우/Desktop/batch/data/etf_pre.csv')

for i in tqdm(range(len(word))):
    f = etf_pre[etf_pre['clean_remark'].str.contains(word.loc[i,'단어'])][['JM_CD', 'JM_NM', 'FUND_CD']].drop_duplicates(subset=['JM_CD', 'FUND_CD'])
    JM_CD = ' / '.join(str(j) for j in f['JM_CD'][:10])
    FUND_CD = ' / '.join(str(j) for j in f['FUND_CD'][:10])
    JM_NM = ' / '.join(str(j) for j in f['JM_NM'][:10])
    word.loc[i,'JM_NM'] = JM_NM
    word.loc[i,'JM_CD'] = JM_CD
    word.loc[i,'FUND_CD'] = FUND_CD

    
# etf_keyword = word[~word.JM_NM.isnull()].reset_index(drop=True)
esg_keyword = word[[len(word.loc[i, 'JM_NM'].split(' / ')) > 1 for i in range(len(word))]].reset_index(drop=True)

esg_keyword.rename(columns={'단어':'KEYWORD', '빈도':'SCORE'}, inplace=True)
esg_keyword['DATE'] = now.strftime('%Y-%m-%d')
esg_keyword['TYPE'] = 'ESG'
esg_keyword = esg_keyword[['DATE', 'TYPE', 'KEYWORD', 'SCORE', 'JM_NM', 'JM_CD', 'FUND_CD']]
esg_keyword = esg_keyword.replace('버스','인버스').replace('차이','차이나').replace('러시','러시아').replace('우드','클라우드').replace('바이','바이오').replace('이노','이노베이션').replace('상하','상하이').replace('플랫','플랫폼').replace('어주','방어주').replace('고배','고배당').replace('버리','레버리지').replace('신재','신재생').replace('리티','모빌리티')
esg_keyword

ff = []
for i in range(len(esg_keyword)):
    k = esg_keyword.iloc[i, 2]
    for j in range(len(esg_keyword.iloc[i, 4].split(' / '))):
        ff.append([k, esg_keyword.iloc[i, 5].split(' / ')[j], esg_keyword.iloc[i, 6].split(' / ')[j], esg_keyword.iloc[i, 4].split(' / ')[j]])

esg_keyword_dupl = pd.DataFrame(ff, columns = ['KEYWORD','JM_CD','FUND_CD','JM_NM'])
esg_keyword_dupl['DATE'] = esg_keyword['DATE'][0]
esg_keyword_dupl['TYPE'] = esg_keyword['TYPE'][0]
esg_keyword_dupl = esg_keyword_dupl[['DATE','TYPE','KEYWORD','JM_CD','FUND_CD','JM_NM']]
esg_keyword_dupl

## keyword 삽입
print('----------------------keyword 삽입----------------------')
conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

for i in esg_keyword.values:
    try:
        insert_query = "INSERT INTO KEYWORD_ETF (DATE, TYPE, KEYWORD, SCORE, JM_NM, JM_CD, FUND_CD) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        sql_data = tuple(i)
        cursor.execute(insert_query, sql_data)
        conn.commit()
    except:
        pass

conn.close()

conn = pymssql.connect(host=r"10.101.34.221", user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

for i in esg_keyword_dupl.values:
    try:
        insert_query = "INSERT INTO KEYWORD_ETF_JM (DATE, TYPE, KEYWORD, JM_CD, FUND_CD, JM_NM) VALUES (%s, %s, %s, %s, %s, %s)"
        sql_data = tuple(i)
        cursor.execute(insert_query, sql_data)
        conn.commit()
    except:
        pass

conn.close()
print('----------------------완료----------------------')

