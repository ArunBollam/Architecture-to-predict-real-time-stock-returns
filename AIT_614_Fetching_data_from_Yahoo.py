
# coding: utf-8

# In[1]:


import bs4 as bs


# In[2]:


import pickle


# In[3]:


import requests


# In[4]:


import datetime as dt


# In[5]:


import time


# In[6]:


import os


# In[7]:


import pandas as pd


# In[8]:


import numpy as np


# In[9]:


import pandas_datareader.data as web


# In[10]:


start = dt.datetime(2006,4,10)
end = dt.datetime(2017,12,15)


# In[11]:


tickers = ['APC','BP','COP','CVX','HES','MRO','OXY','PBR','TOT','VLO','XOM','CL','USO']


# In[16]:


tickers = ['APC','BP','COP','CVX','HES','MRO','OXY','PBR','TOT','VLO','XOM','CL','USO']
if not os.path.exists('stocks1'):
    os.makedirs('stocks1')
final_df = pd.DataFrame(np.zeros((2944, 1)))
for ticker in tickers:
    if not os.path.exists('stocks1/{}.csv'.format(ticker)):
        df = web.DataReader(ticker,'yahoo',start,end)
        returns_df = np.zeros((len(df), 1))
        returns_df = pd.DataFrame(returns_df)
        for i in range(len(df)):
            if i == 0:
                returns_df.iloc[i,0] = 0
            returns_df.iloc[i,0] = (df.iloc[i,4] - df.iloc[i-1,4])/(df.iloc[i-1, 4])
        final_df = pd.concat([final_df,returns_df], axis = 1)
        returns_df.to_csv('stocks1/{}.csv'.format(ticker))
final_df = final_df.iloc[:, 1:]
final_df.columns = ['APC','BP','COP','CVX','HES','MRO','OXY','PBR','TOT','VLO','XOM','CL','USO']       
           


# In[ ]:


final_df.to_csv('total_returns.csv','w')

