#!/usr/bin/env python
# coding: utf-8


import pandas as pd   #  python 3.6+, pd 0.25+
from datetime import datetime, date
from sqlalchemy import create_engine  #sqlite



COVID_DATA = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/'                  'csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'


# In[30]:


df = pd.read_csv(COVID_DATA, skipinitialspace=True).fillna('')
print(df.shape, 'n/a:', pd.isna(df).sum().sum())
df.head()


# In[31]:


# print( df.loc[df['Country/Region']=='France', 'Province/State'] )
df['Country/Region'].value_counts()[:4]


# Some countries have colonies/territories or shown by state/region.  
# Sum that data by country (! check your policy ~French Guiana, Taiwan)
# Dropping "Province" and geo data.  

# In[32]:


df = df.drop(columns=['Province/State', 'Lat', 'Long'])
df = df.rename(columns={'Country/Region': 'country'})
df = df.groupby('country', as_index=False).sum()
print(df.shape)


# #### 2. Python: Convert the table so that each country and each day is a separate row

# In[33]:


df1 = df.melt(id_vars=['country'], value_vars=df.columns[1:], var_name='date', value_name='cum_deaths')
df1['date'] = pd.to_datetime(df1['date'])
df1.reindex()
df1.sample(5)


# #### 3. Python: Provide code to upload the table from step 3 into an SQL table named deaths_total

# In[42]:


# Using sqlite file in the same folder, no credentials
engine = create_engine('sqlite:///covid_d.db')
conn = engine.connect()


# In[35]:


# ! Attention !  Replacing existing table (not append) 
df1.to_sql('deaths_total', con=conn, index=False, if_exists='replace')
#df1.to_sql('deaths_total', con=engine, if_exists='append')


# In[36]:


# just checking
conn.execute("SELECT date, cum_deaths FROM deaths_total WHERE country='Australia' ORDER BY date DESC LIMIT 3").fetchall()


# #### 4. Python: From the data in step 2, calculate the daily change in deaths for each country 

# In[37]:


df2 = df1.copy()

# Daily deaths go to the first day. last day N/A = 0 

for c in df2.country.unique():
    c_ind = df2.country==c
    df2.loc[c_ind, 'day_deaths'] = ( df2.loc[c_ind, 'cum_deaths'].shift(-1) - df2.loc[c_ind, 'cum_deaths'] )

df2.day_deaths = df2.day_deaths.fillna(0).astype('int') 
df2.sample(5)


# #### 5. Python: Provide code to upload the table from step 4 into an SQL table named deaths_change_python

# In[38]:


# ! Attention !  Replacing existing table (not append)
df2.to_sql('deaths_change_python', con=conn, index=False, if_exists='replace')


# #### 6. SQL: Provide SQL code to calculate the daily change for each country using only the data from deaths_total and save it into an SQL table named deaths_change_sql

# In[39]:


#  ! Attention !  Deleting existing table 
conn.execute('DROP TABLE IF EXISTS deaths_change_sql;')


# In[40]:


conn.execute(''' 
    CREATE TABLE deaths_change_sql AS
        SELECT country, date, cum_deaths,
               COALESCE( LEAD(cum_deaths, 1) OVER(PARTITION BY country) - cum_deaths, 0) AS day_deaths 
        FROM deaths_total  
        ORDER BY date, country;
''')


# In[44]:


engine.table_names()


# In[45]:


conn.close()  
print( 'Connection closed:', conn.closed)


