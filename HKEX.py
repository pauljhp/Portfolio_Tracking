# -*- coding: utf-8 -*-
# Copyright (c) 2021, Paul Peng
# An NLP package for extracting meaningful information from HKEXnew.com
# part of the NLP for fundamental analysis package
 
__author__ = 'Paul J. H. Peng'
__author_email__ = 'pauljhpeng@gmail.com'
 
__maintainer__ = 'Paul J. H. Peng'
__maintainer_email__ = __author_email__
 
#import native modules in colab
 
from datetime import date,datetime,time,timedelta
import seaborn as sns
from matplotlib import pyplot as plt
import pandas as pd
from bs4 import BeautifulSoup as bs
import sys, os, re, subprocess, importlib
import numpy as np
import requests, bs4, pip, csv 
from datetime import date, timedelta, datetime
from PyPDF2 import PdfFileReader
from nltk.corpus import stopwords

#checking and install required modules
required_modules={'tabula':'tabula-py',
                'PyPDF2':'PyPDF2',
                'nltk':'nltk',
                'spacy':'spacy',
                'yahooquery':'yahooquery',
                'finnhub':'finnhub-python',
                'docx':'python-docx',
                'google.colab':'google.colab',
                'bs4':'bs4',
                'GoogleNews':'GoogleNews',
                #'feather': 'feather'
                }
default_installation_path='/usr/local/lib/python3.7/dist-packages'
 
for mod in required_modules.items():
    try:
        importlib.import_module(mod[0])
    except ImportError as e:
        print(f'{e}, installing {mod[0]}...')
        cmd=f'pip install {mod[1]} --target {default_installation_path} --upgrade'
        process=subprocess.Popen(cmd.split(),stdout=subprocess.PIPE)
        print(f'result: {process.communicate()}')
print('finished importing')
 
#predefined CSS style for pandas
hover_white=[{'selector': 'tr:hover',
               'props': [('background-color', 'white')]}]
background_white= [{'selector': 'tr:hover',
                    'props': [('color','black'),
                              ('background-color','#32BFFC'),
                              ('text-align','left'),
                              ('font-family', 'arial narrow')]},
                   {'selector':'th',
                    'props':[('color','white'),
                             ('background-color','')],
                    'padding':['15px']
                   }
                  ]
 
#pandas display options
pd.options.display.max_columns=50
pd.options.display.max_rows=200
pd.options.display.max_colwidth=150
pd.options.display.chop_threshold=3
!apt-get update
!apt-get install -y openjdk-8-jdk-headless -qq > /dev/null

!apt install jre-default
!java -version

#set JAVAPATH
!export PATH="$PATH:/usr/java/latest/bin"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

import requests, re, os, json, nltk, tabula
from bs4 import BeautifulSoup

#check of stopwords have been downloaded
try:
    stopwords_list=stopwords.words('english')
except LookupError:
    nltk.download('stopwords')
    stopwords_list=stopwords.words('english')
finally:
    print('downloaded stopwords \n')

class HKEX:
    '''
    extract information from HKEXnews.com and summarize company filings
    '''
    def __init__(self, ticker, period):
        '''
        period need to be int. e.g. previous 7 days - period = 7
        '''
        self._callback = 'https://www1.hkexnews.hk/search/prefix.do?&callback=callback&lang=EN&type=A&name={}&market=SEHK&_=1622544939034'.format(ticker)
        self.ticker = ticker
        self.today = date.today()
        self.todayYYYYMMDD = '{}{}{}'.format(self.today.year, str(self.today.month).zfill(2), str(self.today.day).zfill(2))
        self.start = self.today - timedelta(period)
        self.startYYYYMMDD = '{}{}{}'.format(self.start.year, str(self.start.month).zfill(2), str(self.start.day).zfill(2))
        self.ses = requests.Session()
        self.docu_LUT={'Announcements and Notices': [],
                       'Circulars':[],
                       'Listing Documents':[],
                       'Financial Statements/ESG Information':[],
                       'Next Day Disclosure Returns':[],
                       'Debt and Structured Products':[],
                       'Application Proofs and Post Hearing Information Packs or PHIPs':[],
                       'Monthly Returns':'',
                       'Proxy Forms':'',
                       'Company Infromation Sheet (GEM)':'',
                       'Constitutional Documents': '',
                       'Trading Information Exchange Traded Funds':'',
                       'Regulatory Announcement & News':'',
                       'Share Buyback Reports (Before 1 January 2009)':'',
                       'Takeover Code - dealing disclosures':'',
                       'Trading Information of Leveraged and Inverse Products':''
        }
 
    def _getID(self):
        '''
        :return: the stock ID* if there is a match. can also input a company name
        *this is different from the ticker!
        '''
        headers = {'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    'pragma': 'no-cache',
                    'referer': 'https://www.hkexnews.hk/',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-fetch-dest': 'script',
                    'sec-fetch-mode': 'no-cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}
        params = {'callback': 'callback',
                  'lang': 'EN',
                  'type': 'A',
                  'name': self.ticker,
                  'market': 'SEHK',
                  '_': '1622544939034'}
        res = self.ses.get(self._callback, headers = headers, params = params)
        try:
            ID=json.loads(res.text.replace('callback(', "").replace("\n","").replace(");","").replace("'",""))['stockInfo'][0]
        except TypeError:
            raise Exception("invalid Ticker!")
        except IndexError:
            raise Exception("invalid Ticker!")
        return ID
 
    def _search(self):
        '''
        :return: response object of the search result for a ticker
        '''
        url = 'https://www1.hkexnews.hk/search/titlesearch.xhtml'
        headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7,ja-JP;q=0.6,ja;q=0.5,zh-TW;q=0.4,zh-CN;q=0.3',
                    'cache-control': 'max-age=0',
                    'content-length': '128',
                    'content-type': 'application/x-www-form-urlencoded',
                    'origin': 'https://www.hkexnews.hk',
                    'referer': 'https://www.hkexnews.hk/',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-site',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}   
        data = {'lang': 'EN',
                'market': 'SEHK',
                'searchType': 0,
                #documentType: 
                #t1code: 
                #t2Gcode: 
                #t2code: 
                'stockId': self._getID()['stockId'],
                'from': self.startYYYYMMDD ,
                'to': self.todayYYYYMMDD,
                'category': 0
                }
        res = self.ses.post(url, headers = headers, data = data)
        return res
    
    def search_to_df(self):#,read_content=True):
        '''
        :prams: if read_content is set to False, content of links 
        will not be processed. Otherwise content will be loaded into DataFrame.
        True by default
        :return: dataframe
        '''
        res = self._search()
        df = pd.read_html(res.text)[0]
        df['Release Time'] = [datetime.strptime(i.replace("Release Time: ", ""), "%d/%m/%Y %H:%M") for i in df['Release Time']]
        df['Stock Code'] = [i.replace("Stock Code: ", "") for i in df['Stock Code']]
        df['Stock Short Name'] = [i.replace("Stock Short Name: ", "") for i in df['Stock Short Name']]
        df.Document = [i.replace("Document: ", "") for i in df.Document]
        
        #add title, type and detail info for Document
        Type=pd.Series(dtype=str)
        Title=pd.Series(dtype=str)
        Detail=pd.Series(dtype=str)
        for i,row in df.iterrows():
            string1=str(row.Document)
            #getting first part of string as DocType
            pattern=re.compile(' - ')
            if pattern.search(string1):  #if - exists in string
                matches=[m for m in pattern.finditer(string1)] # in case multiple matches are returned, only process the first one
                for match in pattern.finditer(string1):
                    start,end=matches[0].span()
                    Type.loc[i]=string1[:start].lstrip().rstrip()
                    Remaining=string1[end:]
                #getting text in [] as DocDetails
                pattern=re.compile('\[.*\]')
                if pattern.search(Remaining):
                    for match in pattern.finditer(Remaining):
                        start,end=match.span()
                        Detail.loc[i]=Remaining[start+1:end-1].lstrip().rstrip()
                        Remaining=Remaining[end:]
                else:
                    Detail.loc[i]=None

                #Getting DocTitle, removing (xxxKB)
                pattern=re.compile('\(\d*\wB\)')  #if (xxxKB) exists at the end 
                if pattern.search(Remaining):
                    for match in pattern.finditer(Remaining):
                        start,end=match.span()
                        Title.loc[i]=Remaining[:start-1].lstrip().rstrip()
                else:
                    Title.loc[i]=Remaining
            elif re.search('Monthly Returns',string1,re.IGNORECASE):
                Type.loc[i]='Monthly Returns'
                Remaining=re.sub('Monthly Returns','',string1,re.IGNORECASE)
                Detail.loc[i]=None
                Title.loc[i]=re.sub('\(\d*\wB\)','',Remaining).lstrip().rstrip()
            elif re.search('Proxy Forms',string1,re.IGNORECASE):
                Type.loc[i]='Proxy Forms'
                Remaining=re.sub('Proxy Forms','',string1,re.IGNORECASE)
                Detail.loc[i]=None
                Title.loc[i]=re.sub('\(\d*K\wB\)','',Remaining).lstrip().rstrip()
            elif re.search('Constitutional Documents',string1,re.IGNORECASE):
                Type.loc[i]='Constitutional Documents'
                Remaining=re.sub('Constitutional Documents','',string1,re.IGNORECASE)
                Detail.loc[i]=None
                Title.loc[i]=re.sub('\(\d*\wB\)','',Remaining).lstrip().rstrip()
            else:
                Type.loc[i]=string1
                Title.loc[i]=None
                Detail.loc[i]=None
        
        df['DocType']=Type.values
        df['DocDetails']=Detail.values
        df['DocTitle']=Title.values

        #add link 
        soup = BeautifulSoup(res.text, 'lxml')
        link = pd.Series(dtype=str)

        for i,tr in enumerate(soup.find_all('tr')):
            for rf in tr.find_all('a', href = True):
                link.loc[i]='https://www1.hkexnews.hk'+rf['href']

        df['url'] = link.values
        # print('success!')
        return df

    def getcontent(self):
        '''
        parse through the links, based on the dataframe returned by 
        self.search_to_df
        :return: pandas dataframe
        '''
        df=self.search_to_df()
        _content=pd.Series(dtype=str)
        for i, row in df.iterrows():
            print(f'processing {row.url}...')
            _,ext=os.path.splitext(row.url)
            if ext == '.pdf':
                res=requests.get(row.url)
                with open('temp.pdf', 'wb') as f:
                    f.write(res.content)
                fileobjtemp = PdfFileReader('temp.pdf', strict = False)
                numPage = fileobjtemp.getNumPages()
                #extract headers & content seperately
                #content dict keys are headers preceeding that content
                #headers = []
                #content = {}
                content=''
                for page in range(numPage): 
                    text_raw = fileobjtemp.getPage(page).extractText()
                    content += re.sub(r'[\n\rå\t]',' ',text_raw)
                _content.loc[i]=content
            elif ext=='.htm':
                res=requests.get(row.url)
                soup=bs(res.text,'lxml')
                content=''
                try:
                    for d in soup.find_all('div'):
                        content+=re.sub('[\r\t\n]',' ',d.text)
                except AttributeError:
                    print("no text found!")
                    pass
                _content.loc[i]=content
            else:
                print(f"{df.url} is an invalid type! skipping...")
                _content.loc[i]=content
                pass
        df['Content']=_content.values
        return df

    def freq_count(self):
        '''
        :return: dict of word frequencies
        '''
        df = self.search_to_df()
        freq_dict = {}
        for index, art in df.content.iteritems():
            word_list = art.split(" ") #need to rewrite this to include poorly formatted text (no space in between)
            word_dict = {}
            for word in (word for word in word_list):
                if word not in word_dict.keys():
                    word_dict[word] = 1
                else:
                    word_dict[word] += 1
            freq_dict[index]=word_dict
        freq_count = pd.Series(freq_dict)
        df['word_freq']=freq_count.values #passing freq_count directly works?
        return df

    def gettables(self,include_content=False):
        '''
        extracts tables from PDF filings with tabula
        needs Java and tabula to be installed
        :return: pandas dataframe
        '''
        if include_content:
            df = self.getcontent()
            table=pd.Series(object)
            for i,row in df.iterrows():
                try:
                    table.loc[i]=tabula.read_pdf(row.url, pages='all')
                except Exception:
                    table.loc[i]=None
            df['Tables']=table.values
        else:
            df=self.search_to_df()
            table=pd.Series(object)
            for i,row in df.iterrows():
                try:
                    table.loc[i]=tabula.read_pdf(row.url, pages='all')
                except Exception:
                    table.loc[i]=None
            df['Tables']=table.values
        return df

    def sen_to_TF(self):
        '''
        parses sentenses in the corpus and turn words into their respective term frequencies
        :return: dataframe. Appends list of dict (keys = terms, values = TF) to df
        '''
        df = self.freq_count()
        TF_dict = {}
        for index, row in df.iterrows():
            TF_list =[]
            for sen in (sen for sen in row.content.split(". ")):
                word_list = sen.split(" ")
                temp_dict={}
                for word in word_list:
                    if word not in stopwords_list and word in row.word_freq.keys(): #stopwords not processed 
                        temp_dict[word]=row.word_freq[word]
                    else:
                        temp_dict[word]=None
                TF_list.append(temp_dict)
            TF_dict[index]=TF_list
        df['TF']=pd.Series(TF_dict)
        return df

    def filter(self,target,column_filter={},reversed=False, **kwargs):
        '''
        filters the irrelevant filings
        --------
        :params: 

        target - takes content, tables, or tables_and_content. If set to 
        content, will start from search_to_df, if set to tables, will start from
        gettables. 

        column_filter - takes a dictionary. If unspecified, **kwargs will
        wrap these up.

        reversed - If reversed is set to True, 
        negative selection will be conducted and the function will return
        items that's not in the list specified by the 'filtered' parameter
        
        default setting is False for reversed
        --------
        :return: pandas dataframe
        '''
        if target=='content':
            df=self.search_to_df()
        elif target=='tables':
            df=self.gettables()
        elif target=='tables_and_content':
            df=self.gettables(include_content==True)
        else:
            raise Exception('target can be only content, tables, or tables_and_content')
        
        if column_filter:
            if reversed==False:
                for key, val in column_filter.items():
                        df=df[(df[key].isin(val))]
                else:
                    for key, val in column_filter.items():
                        df=df[(df[key].isin(val))]
        else:
            if reversed==False:
                for key, val in kwargs:
                        df=df[(df[key].isin(val))]
                else:
                    for key, val in kwargs:
                        df=df[(df[key].isin(val))]
        return df

    def report(self):
        df=self.sen_to_TF()
        df.drop()
