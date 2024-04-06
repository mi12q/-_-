#!/usr/bin/env python
# coding: utf-8

# In[203]:


import pandas as pd
from datetime import datetime
import math
import numpy as np
import os,random


# * ### Извлечение данных

# In[204]:


def extract_data(path): 
    """
    Извлекает все данные из папки, по данному пути.
    :param path: - путь
    """
    files = os.listdir(path); csv_files = [file for file in files if file.endswith('.csv')]
    dataframes = {}
    
    for file in csv_files:
        file_path = os.path.join(path, file)
        df = pd.read_csv(file_path, sep = '\t')
        df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x).to_pydatetime())
        dataframes[file] = df

    return dataframes


# In[205]:


all_data = extract_data('/home/milica/тестовое вк/data_for_testing')
data = all_data['variant1.csv']
display(data.sort_values('timestamp'))


# In[206]:


display(data[data['action'] == 'confirmation'].sort_values('timestamp'))


# * ### Обработка данных

# In[207]:


def filter_months_duplicates(df):
    """
    Сортирует таблицу по возрастанию месяцев, в случае нескольких максимальных покупок в месяц выбирает первую из них.
    :param df: - data frame
    """
    df['timestamp'] = df['timestamp'].apply(lambda x: pd.Timestamp(x).to_pydatetime())
    df['year_month'] = df['timestamp'].dt.to_period('M')
    max_by_month = df.groupby('year_month')['value'].transform('max')
    df_max = df[df['value'] == max_by_month]
    df_max_sorted = df_max.sort_values('timestamp').drop_duplicates('year_month',keep="first")

    return df_max_sorted[['timestamp','value']]


# In[208]:


def extract_full_months(df):
    """
    Находит лучший день для каждого полноценного месяца использования сайта. Возращает таблицу с колонками: timestamp, value.
    :param df: - data frame
    """
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date # оставляем только даты, без часов
    users_per_month = df.groupby(['timestamp'])['userid'].unique().reset_index() # группируем пользователей по датам
    full_months = pd.DataFrame(columns=['timestamp','value'])
    min_date, max_date = df['timestamp'].agg(['min', 'max'])
    
    for index, row in users_per_month.iterrows():
        year = row['timestamp'].year; month = row['timestamp'].month
        #проверка на то, является ли месяц первым или последним месяцем, для которого представлены данные
        if (year == min_date.year and month == min_date.month) or (year  == max_date.year and month == max_date.month):
            continue 
            
        unique_users = row['userid']
        #отделяем всех пользователей, которые уже были в таблице  
        old_users = df[(df['timestamp'] < row['timestamp'])]['userid'] 
        #отделяем новых пользователей, пришедших в эту дату
        new_users =  set(unique_users).difference(set(old_users))  
        #находим новых пользователей, совершивших покупки в эту дату
        users_with_conformation = df[(df['userid'].isin(new_users)) & (df['action'] == 'confirmation') & (df['timestamp'] == row['timestamp'])]
        if not users_with_conformation.empty:
                max_user = users_with_conformation.loc[users_with_conformation['value'].idxmax()]
                new_row = {'timestamp': max_user['timestamp'],  'value': max_user['value']}
                full_months.loc[len(full_months)] = new_row
                full_months = full_months.reset_index(drop=True)
     
    full_months = filter_months_duplicates(full_months) 
    
    return full_months


# In[209]:


data1 = extract_full_months(data)
display(data1)


# * ### Тест на 10 случайных выборок

# In[210]:


def test_random_data(n,data):
    for i in range(n):
        key = random.choice(list(data.keys()))
        print("Файл =", key)
        display(extract_full_months(data[key]))


# In[211]:


test_random_data(10,all_data)


# In[ ]:




