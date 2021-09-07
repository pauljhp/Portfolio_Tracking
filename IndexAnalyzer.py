# -*- coding: utf-8 -*-
"""
A Python package for portfolio management and simulations
"""

__author__="Paul J. P. Copyright 2021"

import numpy as np
import os, sys, re
import yahooquery as yq
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt

class Options:
    def __init__(self,put_call,up,down,rf,period=1):
        self._period=period
        self._put_call=put_call
        self._up=up
        self._down=down
        self._rf=rf
    def _Pup(self):
        return (1+self._rf/100-self._down)/(self._up-self._down)
    def _Pdown(self):
        return 1-self._Pup
    
class Index:
    def __init__(self, ticker, start=None, end=None):
        self._start=start
        self._end=end
        self._ticker=ticker
        self._yqTickerobj=yq.Ticker(self._ticker)
    
    @property
    def ticker(self):
        return self._ticker
    @ticker.setter
    def ticker(self, new_ticker):
        self._ticker=new_ticker
        
    @property
    def start(self):
        return self._start
    @start.setter
    def start(self, new_start):
        self._start=new_start
        
    @property
    def end(self):
        return self._end
    @end.setter
    def end(self, new_end):
        self._end=new_end
    def get_price(self):
        return self._yqTickerobj.history(start=self._start,end=self._end,adj_ohlc=True)
    def cal_return(self):
        df=self.get_price()
        df.loc[:,"cum_return"]=[(p/df.close.values[0])**\
                        (1/((d-df.index.get_level_values(1)[0]).days/365.25))-1 \
                            if not d==df.index.get_level_values(1)[0] else np.nan \
                           for (ind,d),p in df.close.iteritems()
                           ]
        return df
    def visualize(self,chart_title=None,omit_period=180,export=False,save_path=None):
        '''visualize the cumulative return
        --------
        parameters:
            imot_period: omit the first periods where return might be volatile
        --------
        returns:
            None
        '''
        df=self.cal_return()
        std=df.iloc[omit_period:,:].describe().cum_return['std']
        mean=df.iloc[omit_period:,:].describe().cum_return['mean']
        mean_p1sd=mean+std
        mean_m1sd=mean-std
        fig,ax=plt.subplots(1,1,figsize=(15,10))
        ax=plt.plot(df.index.get_level_values(1)[omit_period:],df.cum_return[omit_period:],color="blue")
        ax=plt.plot(df.index.get_level_values(1)[omit_period:],[mean for d in df.index.get_level_values(1)[omit_period:]],color="gray")
        ax=plt.plot(df.index.get_level_values(1)[omit_period:],[mean_m1sd for d in df.index.get_level_values(1)[omit_period:]],color="gray", linestyle=":")
        ax=plt.plot(df.index.get_level_values(1)[omit_period:],[mean_p1sd for d in df.index.get_level_values(1)[omit_period:]],color="gray",linestyle=":")
        plt.legend(['return','mean','-std','+1std'])
        plt.title(self._ticker if not chart_title else chart_title)
        if export:
            plt.savefig(os.path.join(save_path,chart_title+'.png'))
        else:
            plt.show()
        return ax
        
    def main(ticker,start):
        from Analyzer import Index
        _=Index(ticker,start,date.today())
        _.visualize()
    
    if __name__=='__main__':
        ticker=input("please enter a ticker: ")
        startd=input("please enter the start period: ")
        print(f"start period set to {date(1990,1,1) if not startd else startd}")
        main(ticker,
             start=date(1990,1,1) if not startd else startd)
    else:
        print("successfully imported!")