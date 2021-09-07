#coding=utf-8

__author__="Paul J. Peng BMO LGM paul.peng@bmo.com Copyight 2021"

import sys, os, re
from xbbg import blp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
#import docx
from datetime import datetime, date, timedelta


class Index:
    def __init__(self,index,currency='USD'):
        self._index=index
        self._currency=currency
    def _getMembWeights(self) ->pd.DataFrame:
        df=blp.bds(self._index,flds=['INDX_MWEIGHT']).sort_values(by='percentage_weight',ascending=False)
        df.index=df.member_ticker_and_exchange_code
        df=df.drop('member_ticker_and_exchange_code',axis=1)
        return df
    def _getShortSellVol(self, period='3m') ->pd.DataFrame:
        '''get member shortsell volume during the specified period
        Parameters
        ----
        period: takes '7d','1m','3m','1y','3y','5y','10y'
        
        returns
        ----
        pd.DataFrame
        '''
        lut={'7d':7,
             '1m':30,
             '3m':90,
             '1y':365,
             '5y':365*5+1,
             '10y':365*10+3,
            }
        weights=self._getMembWeights()
        membs=[t+" Equity" for t in weights.index]
        df=blp.bdh(membs,flds=["short_sell_num_shares"],
                   start_date=datetime.today()-timedelta(days=lut[period]),
                   end_date="today").T
        df['avg']=df.mean(axis=1)
        df['latest_vs_avg']=df.iloc[:,-2]/df.avg
        return df.sort_values(by='latest_vs_avg',ascending=False)
        
    def membMovContr(self,sort='contribution',ascending=False)->pd.DataFrame:
        '''get member movement contribution and volume
        Parameters
        ----
        sort: sort by 'contribution','change',or'weight'
        ascending: False by default
        
        Returns
        ----
        pd.DataFrame
        '''
        weights=self._getMembWeights()
        membs=[t+" Equity" for t in weights.index]
        df=blp.bdp(membs,
                   flds=['security_name','px_last','chg_pct_1d',
                         'px_volume','volume_avg_30d','volume_avg_3m',
                        'volume_avg_6m','gics_sector_name','vwap_turnover'])
        df.index=df.index.str.replace(" Equity","") #remove 'equity' in the index for merging
        df=weights.join(df)
        df['contribution']=df.percentage_weight/100*df.chg_pct_1d
        if sort=='contribution':
            df=df.sort_values(by='contribution',ascending=ascending)
        elif sort=='change':
            df=df.sort_values(by='chg_pct_1d',ascending=ascending)
        elif sort=='weights':
            df=df.sort_values(by='percentage_weight',ascending=ascending)
        
        #add vol calculation
        df['vol_v_30d']=df.px_volume/df.volume_avg_30d-1
        df['vol_v_3m']=df.px_volume/df.volume_avg_3m-1
        df['vol_v_6m']=df.px_volume/df.volume_avg_6m-1
        turn_total=df.vwap_turnover.sum()
        df['turnover_contr']=df.vwap_turnover/turn_total
        return df
    
    def summary(self):
        '''summarizes top movers that contributed to the index by dailiy change and trading activity
        returns
        -------
        string
        '''
        data=self.membMovContr()
        sectors=data.groupby(by='gics_sector_name',axis=0).contribution.sum().sort_values(ascending=False)
        stocks=data.sort_values(by='contribution',ascending=False)
        index_rose=data.contribution.sum()>0
        txt_contributors=f'''{', '.join([f"{i}" for i, row in sectors.head(2).iteritems()])}, and {sectors.index[2]} were among the top sectors that contibuted to performance, based on their daily changes and weights in the index. \nThe top 5 stocks that contributed to performacne were: {', '.join([f"{row.security_name} ({i}, {row.px_last:.2f}, {row.chg_pct_1d:.1f}%)" for i, row in stocks.head(4).iterrows()])}, and {stocks.iloc[4].security_name} ({stocks.iloc[4].name}, {stocks.iloc[4].px_last:.2f}, {stocks.iloc[4].chg_pct_1d:.1f}%);\n'''
        
        txt_detractors=f'''{', '.join([f"{i}" for i, row in sectors.tail(2).iteritems()])}, and {sectors.index[-3]} were among the top sectors that dragged performance, based on their daily changes and weights in the index. \nThe top 5 stocks that dragged index performacne were: {', '.join([f"{row.security_name} ({i}, {row.px_last:.2f}, {row.chg_pct_1d:.1f}%)" for i, row in stocks.tail(4).iterrows()])}, and {stocks.iloc[-5].security_name} ({stocks.iloc[-5].name}, {stocks.iloc[-5].px_last:.2f}, {stocks.iloc[-5].chg_pct_1d:.1f}%);\n'''
        
        abnormal_flows=data.sort_values(by='vol_v_30d',ascending=False).head(5)
        
        txt_activetrd="Trading activities were espetially active on these stocks:\n"+', '.join([f"{row.security_name} ({i}, {row.vol_v_30d*100:.1f}% higher than the past 30-day average)" for i,row in abnormal_flows.iterrows() if row.vol_v_30d>=0.5])
        return txt_contributors+"\nOn the other hand, "+txt_detractors+"\n"+txt_activetrd if index_rose else txt_detractors+"\nOn the other hand, "+txt_contributors+"\n"+txt_activetrd

    
    
def main(exportpath=r'C:\users\jpeng07\desktop',exportname=f'Hang Seng Index {datetime.today().strftime("%Y-%m-%d")}.txt'):
    ins=Index("HSI Index")
    with open(os.path.join(exportpath,exportname),'w') as f:
        f.write(ins.summary())
    
if __name__=='__main__':
    main()
else:
    print("imported module!")