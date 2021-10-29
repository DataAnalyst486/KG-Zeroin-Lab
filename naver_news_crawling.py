#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import urllib.request
from urllib.request import urlopen
from urllib.parse import quote_plus
import requests

import warnings
warnings.filterwarnings('ignore')

import os
import selenium
from selenium import webdriver
import re
from datetime import datetime as dt, timedelta
import datetime
from tqdm import tqdm

import pandas as pd
import time
import math
import numpy as np

from dateutil.parser import parse
import pymssql
from selenium.webdriver import Chrome, ChromeOptions
import lxml
import lxml.html
import time


# In[22]:


yesterday = (datetime.date.today()-timedelta(1)).strftime('%Y%m%d')
# yesterday = '20211010'
print(yesterday)


# In[23]:


def cleansing(text):
            text = text.replace('// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}',"")
        
            text = re.sub('([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})', '', text)
            text = re.sub("[\n\t]", '', text)
            text = re.sub(r"\[.*\]|\s-\s.*", "", text) # [ ] 와 사이의 문자 제거



            text = re.sub("(=*([가-힣]{2,4}|[가-힣]{6}) 기자(... [가-힣]{2,3}기자|[^가-힣]|$)=*|로봇 기자)",r' 기자\3 ',text) # 000기자 -> 기자로 변환
            text = re.sub('(‘|\")(.+)(’|\")',r" '\2' ", text)                     # ' " ’ -> 로  변환
            text = re.sub('【(.+)】',r' [\1] ', text)                                #【( 등과 같은 괄호 '[ ]' 로 변환
            #text = re.sub('[0-9]+.[0-9]+%|[0-9]+.[0-9]+%|[0-9]+%',' 비율 ', text)  #12% , 00% -> '비율'로  변환
            # text = re.sub('\w+\([0-9]+\)',' stock ', text)    # 삼성전자[12345] , 카카오[234421] -> '주식종목' 로 변환
            text = re.sub('(http|ftp|https)://((\w+\.|\w+/|\w+|(\?|=|\&)\w+)+)', ' 주소 ', text)  #url -> '주소' 로 변환
            text = re.sub('[0-9]+-[0-9]+|02-[0-9]+-[0-9]+',' 번호 ', text)        #000-0000-0000 -> '번호'로 변환
            text = re.sub('TOP [0-9]+|TOP[0-9]+',' 순위', text)                    # TOP 1, TOP 10 -> '순위'로 변환
            #text = re.sub('([0-9]+|[0-9]+\s|\w*백|\w*천)(억|만원|조)',' 돈 ', text)  # 3백만원, 3천억, 3천조 -> '돈' 로변환
            text = re.sub('[0-9]+:[0-9]+|[0-9]+시 [0-9]분',' 시간 ', text)         # 12:12 , 12시 12 분 -> '시간'로 변환
            #text = re.sub('[0-9]+월|[0-9]+개월|[0-9]+ 개월',' 월 ', text)       # 00월 , 00개월, 00 개월 - >'월' 로 변환
            #text = re.sub('[0-9]+일',' 일 ', text)                                # [0-9]일 -> '일'로 변환
            #text = re.sub('[0-9]+년|2020',' 연도 ', text)                          # 00년 ,2020 -> '연도'로 변환
            #text = re.sub('([0-9]+|[0-9]+\s)배',' 배수 ', text)                     # 00배 , 00 배 -> '배수'로 변환
            text = re.sub('코로나19|코로나 19',' 코로나 ', text)                   # 코로나19, 코로나 19 -> '코로나' 로 변환
            text = re.sub('(\[\w+)(=)(\w+\])',r'\1 \2 \3',text )                # 해럴드경제(대전)=홍길동기자 ->해럴드경제 (대전) 홍길동기자        
            text = re.sub("(\w+)(\")(\w+)",r"\1 \2 \3",text)                    
            text = re.sub('(&nbsp;)|(&nbsp)|(&rsquo;)|(&rsquo)|(&lsquo;)|(&lsquo)|(&middot;)|(&middot)|(&#[0-9]+;)|(fffff;)|\x03|\n',"",text) # css 공통 문법 제거
            #text = re.sub('(&lsquo(;)',"",text) # css 공통 문법 제거            
            text = re.sub('(<([^>]+)>)', '', text)                              # html 문법 제거하기 태그포함
#             text = re.sub('[]', '', text)
            text = re.sub("[']", '', text)
            text = re.sub('quot|rdquo|ldquo', '"', text)
            text = re.sub('[-=+,#/▶▲●◆◇■◀\?.:^$@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》☞△&·]', ' ', text) # 특문제거
            # text = re.sub('[0-9]{6}',
            re.findall('\d+', text)

            if re.search('[a-zA-Z]+-[a-zA-Z]+(:|;)',text)!=None:
                text = " ".join(re.findall('[가-힣]+|[0-9]+[가-힣]+',text)) #css 문법(font-family 등)이 있는 경우에는 한글/숫자+한글인 것만 추출
            # if re.search('[가-힣]+\[[0-9]+\]', text)!=None:
            #   text = re.split('(\[)',text)
            text = re.sub('\;', "", text)
            text = re.sub('[-_+]|돋움|&[a-zA-Z]{2,100};'," ",text)          # 돋움이라는 font가 들어있는 경우는 css에서 제거하기 힘들어서 별도로 제거 + &quot같은 것들도 제거
            text = re.sub('(\w+)(\s\.\.+|\.\.+)',"",text)     # ... , ..  -> 공백으로 변환
            # text = re.sub('\.',"",text)                                    # 마침표 제거 
            text = re.sub('\s\s+'," ",text)                                # 공백 2개이상 -> 한개로
            # text = re.sub('[^0-9가-힣]', ' ', text)

            if re.search('\S',text)==None:                                          # 텍스트가 비어있으면 '빈칸'로 채움
                text = 'blank' 

            return text


# In[ ]:
stopword = [
    '아직도', '추천주', '무단전재', '재배포', '금지', '및', '돈내고', '받으세요', '클릭', '이젠', '카톡방', '받는다', '아직', '무료', '한국경제',
    '아시아경제', 'Global Economy', '이데일리', '매일경제', '연합뉴스', '정보통신신문', '아이투자', '헬스코리아', '뉴스와이어', '델톤', '.com', '아이투자',
    '저작권자 ',
    'www.itooza.com', '무단전재', '재배포', '금지', '한국경제', '무료', '카톡방', '추천주', '추천', '클릭', '번호', '하루만에', '신청즉시', '관심종목', '뉴스',
    '대장주',
    '즉시참여하기', '신청폭주상', '빠른신청은', '카운트 다운', '종목명', '계좌', '한국경제신문', '금지', '기자', 'hankyung.com', '아시아경제', 'ioy asiae.co.kr',
    'quot',
    '받으세요', '광고', '뉴스카페', 'ⓒ', 'ace', '매경이코노미', '매일경제', '무단전재 및 재배포 금지 ', 'mk.co.kr', '\\n☞', 'asiae.co.kr', '종합 경제정보',
    '뉴스 앱',
    '속보', '주목하세요', '적중', '터질', '종목', '주목', '카카오톡', '드립니다', '기사', '관련', '무조건', '대박', '비율', '연', '월', '일', '훨훨', '따라오',
    '체험', '선착순',
    '특급', '터질', '오늘', '초보', '기업', '확인', '증권가', '무조건', '연도', '명이', '지금', '당장', '이것', '사라', '주린', '리딩', '평생', '바로', '입장',
    '남발', '여의도',
    '궁금', '체험', '한가', '폭탄', '한경', '닷컴', '정보', '투자', '방송', '전문가', '종합', '주식', '역대', '분기', '금융', '신청', '실시간', '당신', '미디어',
    '합니다',
    '시험', '탁론', '모바일', '플러스', '본드', '마켓', '언제', '트래블러', '어디', '생활', '신문', '포인트', '위한', '분야', '기반', '통해', '위해', '한다',
    '그룹',
    '브랜드', '스타', '된다', '위원회', '해야', '경우', '대한', '때문', '국민', '따르', '연구원', '수수료', '대출', '없이', '증권사', '신용', '저작권자', '컨텐츠',
    '최신', '입니다', '즉시', '문자', '발송', '내일', '상담', '링크', '고민', '타이밍', '여러분', '이제', '모르', '마감', '간편', '한국', '즐거움', '온라인',
    '기관', '대비',
    '알고리즘', '그래프', '최근', '작성', '해당', '관심', '근거', '참고', '포착', '대해', '인공', '마지막', '최근', '코리아', '스타일', '보도', '직원', '황제',
    '탄생', '행진',
    '트렌드', '진단', '이날', '전년', '부문', '대표', '비실비실대다', 'i레터', '불리우는', '대국민', '비실비실대', '레터', '옴디아', '무단', '전재', '배포', '관련주',
    '련주', '카드뉴스',
    '피쉬', '칩스', '신기록(종합2보)', '연합뉴스', '에프앤가이드', 'MARKETPOINT', '명예', '전당','대상'
]

stopword_title = [
    '아직도', '추천주', '무단전재', '재배포', '금지', '및', '돈내고', '받으세요', '클릭', '이젠', '카톡방', '받는다', '아직', '무료', '한국경제',
    '아시아경제', 'Global Economy', '이데일리', '매일경제', '연합뉴스', '정보통신신문', '아이투자', '헬스코리아', '뉴스와이어', '델톤', '.com', '아이투자',
    '저작권자 ',
    'www.itooza.com', '무단전재', '재배포', '금지', '한국경제', '무료', '카톡방', '추천주', '추천', '클릭', '번호', '하루만에', '신청즉시', '관심종목', '뉴스',
    '대장주',
    '즉시참여하기', '신청폭주상', '빠른신청은', '카운트 다운', '종목명', '계좌', '한국경제신문', '금지', '기자', 'hankyung.com', '아시아경제', 'ioy asiae.co.kr',
    'quot',
    '받으세요', '광고', '뉴스카페', 'ⓒ', 'ace', '매경이코노미', '매일경제', '무단전재 및 재배포 금지 ', 'mk.co.kr', '\\n☞', 'asiae.co.kr', '종합 경제정보',
    '뉴스 앱',
    '속보', '주목하세요', '적중', '터질', '종목', '주목', '카카오톡', '드립니다', '기사', '관련', '무조건', '대박', '비율', '연', '월', '일', '훨훨', '따라오',
    '체험', '선착순',
    '특급', '터질', '오늘', '초보', '기업', '확인', '증권가', '무조건', '연도', '명이', '지금', '당장', '이것', '사라', '주린', '리딩', '평생', '바로', '입장',
    '남발', '여의도',
    '궁금', '체험', '한가', '폭탄', '한경', '닷컴', '정보', '투자', '방송', '전문가', '종합', '주식', '역대', '분기', '금융', '신청', '실시간', '당신', '미디어',
    '합니다',
    '시험', '탁론', '모바일', '플러스', '본드', '마켓', '언제', '트래블러', '어디', '생활', '신문', '포인트', '위한', '분야', '기반', '통해', '위해', '한다',
    '그룹',
    '브랜드', '스타', '된다', '위원회', '해야', '경우', '대한', '때문', '국민', '따르', '연구원', '수수료', '대출', '없이', '증권사', '신용', '저작권자', '컨텐츠',
    'MARKETPOINT',
    '최신', '입니다', '즉시', '문자', '발송', '내일', '상담', '링크', '고민', '타이밍', '여러분', '이제', '모르', '마감', '간편', '한국', '즐거움', '온라인',
    '기관', '대비',
    '알고리즘', '그래프', '최근', '작성', '해당', '관심', '근거', '참고', '포착', '대해', '인공', '마지막', '최근', '코리아', '스타일', '보도', '직원', '황제',
    '탄생', '행진',
    '트렌드', '진단', '이날', '전년', '부문', '대표', '비실비실대다', 'i레터', '불리우는', '대국민', '비실비실대', '레터', '옴디아', '무단', '전재', '배포', '관련주',
    '련주', '카드뉴스',
    '피쉬', '칩스', '신기록(종합2보)', '연합뉴스', '에프앤가이드', '증권', '경제', '기술', '인기', '신고', '매수', '코스피', '코스닥', '글로벌', '급등', '은행',
    '정배', '중배', '포토', '상품',
    '개발', '코로나', '식품', '외국인', '할인', '천주', '간추', '이벤트', '진행', '용품', '업종', '스토리', '조재영', '경신', '시황', '가치주', '출시', '시장',
    '외환', '상장지수', '추종', '명예', '전당','대상'
]

# In[212]:


from eunjeon import Mecab

mecab = Mecab('C:/mecab/mecab-ko-dic')

# In[213]:


tag_classes = ['NNG', 'NNP', 'VA', 'VV+EC', 'XSV+EP', 'XSV+EF', 'XSV+EC', 'VV+ETM', 'MAG', 'MAJ', 'NP', 'NNBC', 'IC',
               'XR', 'VA+EC', 'SL']


def extract_noun_mecab(text):  # 형태소중 '조사'를 제외하고 clean_word에 추가.
    clean_word = []
    result = mecab.pos(text)
    for word, tag in result:
        if tag in tag_classes:
            if (word not in stopword) and len(word) > 1:
                clean_word.append(word)

    return " ".join(clean_word)


# In[214]:


def extract_noun_mecab_title(text):  # 형태소중 '조사'를 제외하고 clean_word에 추가.
    clean_word = []
    result = mecab.pos(text)
    for word, tag in result:
        if tag in tag_classes:
            if (word not in stopword_title) and len(word) > 1:
                clean_word.append(word)

    return " ".join(clean_word)
# In[20]:


def summary_news_crawling(date=yesterday):
    
    op = ChromeOptions()
    op.add_argument('--headless')
    op.add_argument('--no-sandbox')
    op.add_argument('--disable-dev-shm-usage')

    browser = Chrome('C:/Users/남건우/Desktop/batch/data/chromedriver.exe', options=op)
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.54 Safari/537.36'
    
    url = "https://news.naver.com/main/list.naver"

    header ={

        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'

    }
    
    article_info = pd.DataFrame()
    
    # 경제 부문만
    category='경제'

#     # 시작 날짜부터 마지막 날짜까지
#     for date in range(start_date, end_date+1):

    # 마지막 페이지 번호 구하기
    for first_page in range(1,1000, 10):

        param = {
            "mode" : "LSD",
            "mid" : "sec",
            "sid1" : "101",
            "listType" : "summary", # 속보 중 "요약" 부문 가져오기
            "date" : f"{date}",
            "page" : f"{first_page}"
        }


        # 들어갈 링크 가져오기
        response = requests.get(url,params=param, headers=header).text
        soup = BeautifulSoup(response, 'html.parser')

        # "다음" 버튼이 있는지 없는지로 판단
        pre_btn = soup.find('a', class_='pre nclicks(fls.page)')
        next_btn = soup.find('a', class_="next nclicks(fls.page)")

        if next_btn != None:
            continue

        else:
            try:
                last_page = int(soup.find_all("a", class_="nclicks(fls.page)")[-1].text)
                print(f"{category} {date} 일자 최종 페이지 번호는 {last_page}입니다")                
                break

            except:
                paging_div = soup.find('div', class_='paging')
                last_page = int(paging_div.find_all('strong')[-1].text)
                print(f"{category} {date} 일자 최종 페이지 번호는 {last_page}입니다")                
                break

        time.sleep(1)

    # 마지막 페이지까지 기사별 링크 가져오기
    all_news_links = []

    for page in range(1, last_page+1): #last_page+1

        param = {
            "mode" : "LSD",
            "mid" : "sec",
            "sid1" : "101",
            "listType" : "summary",
            "date" : f"{date}",
            "page" : f"{page}"
        }

        # 아무런 기사가 없는 날에도 page는 1로 존재하는 경우에 대한 처리

        try:
            # 들어갈 링크 가져오기
            response = requests.get(url,params=param, headers=header).text
            soup = BeautifulSoup(response, 'html.parser')


            # 이미지가 있는 경우 하나의 기사당 모두 3개의 동일한 href를 가짐 - class명 등 구분 방법이 따로 없어 모두 가져온 후 set() 처리
            page_news_links = soup.find_all("a", class_="nclicks(fls.list)") # link명이 기사용과 다름


            for page_news_link in page_news_links:
                all_news_links.append(page_news_link.attrs['href'])

            time.sleep(1)

        except:
            break

    all_news_links = set(all_news_links)

    print("기사별 링크 크롤링 완료")

    time.sleep(1)


    # 실제 기사 속 내용 모두 가져오기

#             article_info = pd.DataFrame()

    for i, news_link in enumerate(tqdm(all_news_links)):

        try:
            # 기사 속 내용 가져오기
            time.sleep(0.5)
            article_response = requests.get(news_link, headers=header).text
            article_soup = BeautifulSoup(article_response, 'html.parser')

            browser.get(news_link)
            time.sleep(0.5)

            html = browser.page_source
            root = lxml.html.fromstring(html)

        except:
            pass
        

        # 잘못된 url이 있는 경우가 존재했음 - pass로 제외하고 진행
        try:               

            news_title = article_soup.find('h3', id='articleTitle').text
            news_title = re.sub(r"\[.*\]|\s-\s.*", "", news_title)

            news_date = article_soup.find('span', class_='t11').text

            url_detail = article_soup.find('a', class_='btn_artialoriginal').attrs['href']
            contents = article_soup.find('div', class_="_article_body_contents").text
    #                     article = re.sub("[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》\n\t]", '', article_soup.find('div', class_="_article_body_contents").text)
    #                     article = re.sub(' +', ' ', article).lstrip()
            contents = contents.replace('// flash 오류를 우회하기 위한 함수 추가 function _flash_removeCallback() {}',"")

        # 좋아요 / 화나요 크롤링 코드 추가
            good_body = root.cssselect('#spiLayer > div._reactionModule.u_likeit > ul > li.u_likeit_list.good > a > span.u_likeit_list_count._count')
            good = good_body[0].text_content()
            bad_body = root.cssselect('#spiLayer > div._reactionModule.u_likeit > ul > li.u_likeit_list.angry > a > span.u_likeit_list_count._count')
            bad = bad_body[0].text_content()
            source_body = root.cssselect('#wrap > table > tbody > tr > td.aside > div > div:nth-child(1) > h4 > em')
            source = source_body[0].text_content()


            article_info = article_info.append(pd.DataFrame([[category, news_title, news_date, url_detail, contents, good, bad,source]], columns=['CATEGORY', 'TITLE', 'NEWS_DATE', 'URL_DETAIL', 'clean_content', 'good', 'bad','source'])).reset_index(drop=True)
            # article_info = article_info.append(pd.DataFrame([[good, bad]], columns=['good', 'bad'])).reset_index(drop=True)

        except:
            print('실패')
            pass

    # 형식 추가 (날짜 형식 수정 + 날짜별 내림차순 + 날짜와 순번을 결합한 IDX 추가)
    article_info['NEWS_DATE'] = article_info['NEWS_DATE'].apply(lambda x: x.replace('오후','PM').replace('오전','AM'))
    article_info['NEWS_DATE'] = article_info['NEWS_DATE'].apply(lambda x: datetime.datetime.strptime(x,"%Y.%m.%d. %p %I:%M"))



    article_info = article_info.sort_values('NEWS_DATE',ascending=False).reset_index(drop=True)
    article_info['IDX'] = [date.strftime('%y%m%d') + '{0:04d}'.format(i+1) for i,date in enumerate(article_info['NEWS_DATE'])]
    article_info['clean_content'] = article_info['clean_content'].apply(lambda x: cleansing(x))
    article_info['final_title'] = article_info['TITLE'].apply(lambda x: cleansing(x))
    article_info['final_title'] = article_info['final_title'].apply(lambda x: extract_noun_mecab_title(x))
    article_info['final_content'] = article_info['clean_content'].apply(lambda x: extract_noun_mecab(x))

    article_info = article_info[['IDX','CATEGORY','NEWS_DATE','TITLE','clean_content','URL_DETAIL','final_title', 'final_content', 'good', 'bad','source']]







    article_info.to_csv(f"C:/Users/남건우/Desktop/batch/data/news/article_{date}.csv", encoding='utf-8-sig', index=False)
    


# In[21]:


summary_news_crawling()






articles = pd.read_csv(f"C:/Users/남건우/Desktop/batch/data/news/article_{yesterday}.csv")

articles.info()


# In[ ]:


articles


# In[ ]:


if articles['good'].dtype=='O' and articles['bad'].dtype=='O':
    articles['good'] = articles['good'].apply(lambda x: int(x.replace(',', '')))
    articles['bad'] = articles['bad'].apply(lambda x: int(x.replace(',', '')))
    
elif articles['good'].dtype=='O' and articles['bad'].dtype!='O':
    articles['good'] = articles['good'].apply(lambda x: int(x.replace(',', '')))
    
elif articles['good'].dtype!='O' and articles['bad'].dtype=='O':
    articles['bad'] = articles['bad'].apply(lambda x: int(x.replace(',', '')))

articles.fillna('', inplace=True)

articles.info()


# In[ ]:


conn = pymssql.connect(host=r'10.101.34.221', user='sa', password='vmfkdla2006', database='BWPRIME', charset='UTF-8')
cursor = conn.cursor()

insert_query = "INSERT INTO NEWS_LIST_NAVER (IDX, CATEGORY, NEWS_DATE, TITLE, clean_content, URL_DETAIL, final_title, final_content,  good, bad, source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


# In[ ]:


points = math.ceil(len(articles)/100)
points


# In[ ]:


for i in tqdm(range(points)):
    df = [tuple(x) for x in articles.iloc[i*100:(i+1)*100].values]
    cursor.executemany(insert_query, df)

conn.commit()
conn.close()


# In[ ]:




