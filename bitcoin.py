#!/usr/bin/env python
# coding: utf-8

# In[51]:


# necessary packages
import pandas as pd
from pandas.io.json import json_normalize
import gzip
import json
import multiprocessing as mp


# In[52]:


def convert_json(json_str):
#     converts json string to df
    return json_normalize(json.loads(json_str))

def convert_data(prefix, file_num):
    for i in range(file_num):
        suffix = str(i).zfill(12)
        filename = prefix + suffix
        
        with gzip.open(filename, "rt", encoding = "utf-8") as file:
            with mp.Pool() as pool:
                results = pool.map(convert_json, [line for line in file if line])
                mode = 'a' if i else 'w'
                pd.concat(results, sort=False).to_csv('data.csv', header = bool(i), mode = mode)        


# In[ ]:


# convert gunzipped json files from google bigquery to a single csv
convert_data('data/2019_01_04_', 4)


# In[ ]:




