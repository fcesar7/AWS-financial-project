import pandas as pd
import requests
from datetime import datetime
from io import StringIO
import boto3
from fredapi import Fred
from alpha_vantage.timeseries import TimeSeries
import os
import json

s3 = boto3.client('s3')

# AWS S3 configuration
S3_BUCKET = 'financial-project-1'
S3_PREFIX = 'extraction-staging/'

def read_s3_file(key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj['Body'])

def write_s3_file(key, df):
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())

### Extraction ###

# Interest Rates
# Note: there is no way of specifying start date for this security with this API
def create_us_rates(key_AV):
    function = 'FEDERAL_FUNDS_RATE'   # Effective Federal Funds Rate
    url = f'https://www.alphavantage.co/query?function={function}&apikey={key_AV}'
    r = requests.get(url)
    data = r.json()
    interest_rates = pd.DataFrame(data)
    interest_rates = interest_rates['data'].apply(lambda x: pd.Series(x))
    interest_rates.rename(columns={'value':'us_rates_%'},inplace=True)
    interest_rates['date'] = pd.to_datetime(interest_rates['date'],format='%Y-%m-%d')
    interest_rates['us_rates_%'] = interest_rates['us_rates_%'].astype(float)
    interest_rates = interest_rates.sort_values('date').reset_index(drop=True)
    interest_rates = interest_rates[interest_rates['date']>='2000-01-01']
    # create date range from 2000 until today and fill gaps
    start_date = '2000-01-01'
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    us_rates = date_range.merge(interest_rates,how='outer',on='date')
    us_rates = us_rates.ffill()
    write_s3_file(S3_PREFIX + "us_rates.csv", us_rates)

# S&P 500 
def create_snp(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'SPY'  # S&P 500 ETF
    try:
        hist = read_s3_file(S3_PREFIX + "snp.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10] # extract since last 10th available date (possible updates in recent data)
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    sp500 = pd.DataFrame(data)
    sp500 = pd.concat([sp500['Meta Data'],sp500['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    sp500 = sp500.reset_index()
    sp500 = sp500.iloc[5:]
    sp500.drop(columns=['Meta Data',0],inplace=True)
    sp500.rename(columns={'index':'date'},inplace=True)
    sp500.columns = [f'sp500{col[2:]}' if i >= 1 else col for i, col in enumerate(sp500.columns)]
    sp500['date'] = pd.to_datetime(sp500['date'],format='%Y-%m-%d')
    sp500['sp500 open'] = pd.to_numeric(sp500['sp500 open'], errors='coerce')
    sp500['sp500 high'] = pd.to_numeric(sp500['sp500 high'], errors='coerce')
    sp500['sp500 low'] = pd.to_numeric(sp500['sp500 low'], errors='coerce')
    sp500['sp500 close'] = pd.to_numeric(sp500['sp500 close'], errors='coerce')
    sp500['sp500 volume'] = pd.to_numeric(sp500['sp500 volume'], errors='coerce')
    sp500['sp500 high-low'] = sp500['sp500 high'] - sp500['sp500 low']
    sp500 = sp500.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    sp500 = date_range.merge(sp500,how='outer',on='date') # get date range from 2000 until today and fill gaps
    sp500 = sp500.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    sp500['type'] = 'new'
    hist['type'] = 'history'
    snp = pd.concat([sp500,hist])
    snp[snp.duplicated(subset=['date'],keep=False)] = snp[(snp.duplicated(subset=['date'],keep=False))&(snp['type']=='new')]
    snp = snp[snp['date'].isna()==False]
    snp.drop(columns='type',inplace=True)
    snp = snp[snp['date']>='2000-01-01']
    snp = snp.sort_values('date')
    snp = snp.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "snp.csv", snp)

# NASDAQ 100
def create_nasdaq(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'QQQ'  # NASDAQ 100 ETF
    try:
        hist = read_s3_file(S3_PREFIX + "nasdaq.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    nsdq = pd.DataFrame(data)
    nsdq = pd.concat([nsdq['Meta Data'],nsdq['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    nsdq = nsdq.reset_index()
    nsdq = nsdq.iloc[5:]
    nsdq.drop(columns=['Meta Data',0],inplace=True)
    nsdq.rename(columns={'index':'date'},inplace=True)
    nsdq.columns = [f'nasdaq{col[2:]}' if i >= 1 else col for i, col in enumerate(nsdq.columns)]
    nsdq['date'] = pd.to_datetime(nsdq['date'],format='%Y-%m-%d')
    nsdq['nasdaq open'] = pd.to_numeric(nsdq['nasdaq open'], errors='coerce')
    nsdq['nasdaq high'] = pd.to_numeric(nsdq['nasdaq high'], errors='coerce')
    nsdq['nasdaq low'] = pd.to_numeric(nsdq['nasdaq low'], errors='coerce')
    nsdq['nasdaq close'] = pd.to_numeric(nsdq['nasdaq close'], errors='coerce')
    nsdq['nasdaq volume'] = pd.to_numeric(nsdq['nasdaq volume'], errors='coerce')
    nsdq['nasdaq high-low'] = nsdq['nasdaq high'] - nsdq['nasdaq low']
    nsdq = nsdq.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    nsdq = date_range.merge(nsdq,how='outer',on='date') # get date range from 2000 until today and fill gaps
    nsdq = nsdq.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    nsdq['type'] = 'new'
    hist['type'] = 'history'
    nasdaq = pd.concat([nsdq,hist])
    nasdaq[nasdaq.duplicated(subset=['date'],keep=False)] = nasdaq[(nasdaq.duplicated(subset=['date'],keep=False))&(nasdaq['type']=='new')]
    nasdaq = nasdaq[nasdaq['date'].isna()==False]
    nasdaq.drop(columns='type',inplace=True)
    nasdaq = nasdaq[nasdaq['date']>='2000-01-01']
    nasdaq = nasdaq.sort_values('date')
    nasdaq = nasdaq.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "nasdaq.csv", nasdaq)

# Consumer Price Index
def create_cpi(key_FRED):
    try:
        hist = read_s3_file(S3_PREFIX + "cpi.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-80]
        start_date = start_date.strftime('%Y-%m-%d')
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()    
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    key = key_FRED
    fred = Fred(api_key=key)
    series_id = 'CPIAUCSL'
    cpi_data = fred.get_series(series_id, start_date=f'{start_date}', end_date=f'{end_date}')
    cpi_data = pd.DataFrame(cpi_data).reset_index()
    cpi_data.rename(columns={'index':'date',0:'CPI'},inplace=True)
    cpi_data = cpi_data[cpi_data['date']>='2000-01-01']
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    cpi_data = date_range.merge(cpi_data,how='outer',on='date')
    cpi_data = cpi_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    cpi_data['type'] = 'new'
    hist['type'] = 'history'
    cpi = pd.concat([cpi_data,hist])
    cpi[cpi.duplicated(subset=['date'],keep=False)] = cpi[(cpi.duplicated(subset=['date'],keep=False))&(cpi['type']=='new')]
    cpi = cpi[cpi['date'].isna()==False]
    cpi.drop(columns='type',inplace=True)
    cpi = cpi.sort_values(by='date')
    cpi = cpi.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "cpi.csv", cpi)

# USD / CHF
def create_usd_chf(key_FRED): 
    try:
        hist = read_s3_file(S3_PREFIX + "usd_chf.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        start_date = start_date.strftime('%Y-%m-%d')
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()    
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    key = key_FRED
    fred = Fred(api_key=key)
    series_id = 'DEXSZUS'
    usd_chf_data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
    usd_chf_data = pd.DataFrame(usd_chf_data).reset_index()
    usd_chf_data.rename(columns={'index':'date',0:'usd_chf'},inplace=True)
    usd_chf_data = usd_chf_data[usd_chf_data['date']>='2000-01-01']
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    usd_chf_data = date_range.merge(usd_chf_data,how='outer',on='date')
    usd_chf_data = usd_chf_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    usd_chf_data['type'] = 'new'
    hist['type'] = 'history'
    usd_chf = pd.concat([usd_chf_data,hist])
    usd_chf[usd_chf.duplicated(subset=['date'],keep=False)] = usd_chf[(usd_chf.duplicated(subset=['date'],keep=False))&(usd_chf['type']=='new')]
    usd_chf = usd_chf[usd_chf['date'].isna()==False]
    usd_chf.drop(columns='type',inplace=True)
    usd_chf = usd_chf.sort_values(by='date')
    usd_chf = usd_chf.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "usd_chf.csv", usd_chf)

# EUR / USD
def create_eur_usd(key_FRED):
    try:
        hist = read_s3_file(S3_PREFIX + "eur_usd.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        start_date = start_date.strftime('%Y-%m-%d')
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()    
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    key = key_FRED
    fred = Fred(api_key=key)
    series_id = 'DEXUSEU'
    eur_usd_data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
    eur_usd_data = pd.DataFrame(eur_usd_data).reset_index()
    eur_usd_data.rename(columns={'index':'date',0:'eur_usd'},inplace=True)
    eur_usd_data = eur_usd_data[eur_usd_data['date']>='2000-01-01']
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    eur_usd_data = date_range.merge(eur_usd_data,how='outer',on='date')
    eur_usd_data = eur_usd_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    eur_usd_data['type'] = 'new'
    hist['type'] = 'history'
    eur_usd = pd.concat([eur_usd_data,hist])
    eur_usd[eur_usd.duplicated(subset=['date'],keep=False)] = eur_usd[(eur_usd.duplicated(subset=['date'],keep=False))&(eur_usd['type']=='new')]
    eur_usd = eur_usd[eur_usd['date'].isna()==False]
    eur_usd.drop(columns='type',inplace=True)
    eur_usd = eur_usd.sort_values(by='date')
    eur_usd = eur_usd.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "eur_usd.csv", eur_usd)

# GDP
def create_gdp(key_FRED):
    try:
        hist = read_s3_file(S3_PREFIX + "gdp.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-110]
        start_date = start_date.strftime('%Y-%m-%d')
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()    
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    key = key_FRED
    fred = Fred(api_key=key)
    series_id = 'GDP' # Gross Domestic Product
    gdp_data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
    gdp_data = pd.DataFrame(gdp_data).reset_index()
    gdp_data.rename(columns={'index':'date',0:'GDP'},inplace=True)
    gdp_data = gdp_data[gdp_data['date']>='2000-01-01']
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    gdp_data = date_range.merge(gdp_data,how='outer',on='date')
    gdp_data = gdp_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    gdp_data['type'] = 'new'
    hist['type'] = 'history'
    gdp = pd.concat([gdp_data,hist])
    gdp[gdp.duplicated(subset=['date'],keep=False)] = gdp[(gdp.duplicated(subset=['date'],keep=False))&(gdp['type']=='new')]
    gdp = gdp[gdp['date'].isna()==False]
    gdp.drop(columns='type',inplace=True)
    gdp = gdp.sort_values(by='date')
    gdp = gdp.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "gdp.csv", gdp)


def create_silver(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'SIVR'  # silver 
    try:
        hist = read_s3_file(S3_PREFIX + "silver.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    silver_data = pd.DataFrame(data)
    silver_data = pd.concat([silver_data['Meta Data'],silver_data['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    silver_data = silver_data.reset_index()
    silver_data = silver_data.iloc[5:]
    silver_data.drop(columns=['Meta Data',0],inplace=True)
    silver_data.rename(columns={'index':'date'},inplace=True)
    silver_data.columns = [f'silver{col[2:]}' if i >= 1 else col for i, col in enumerate(silver_data.columns)]
    silver_data['date'] = pd.to_datetime(silver_data['date'],format='%Y-%m-%d')
    silver_data['silver open'] = pd.to_numeric(silver_data['silver open'], errors='coerce')
    silver_data['silver high'] = pd.to_numeric(silver_data['silver high'], errors='coerce')
    silver_data['silver low'] = pd.to_numeric(silver_data['silver low'], errors='coerce')
    silver_data['silver close'] = pd.to_numeric(silver_data['silver close'], errors='coerce')
    silver_data['silver volume'] = pd.to_numeric(silver_data['silver volume'], errors='coerce')
    silver_data['silver high-low'] = silver_data['silver high'] - silver_data['silver low']
    silver_data = silver_data.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    silver_data = date_range.merge(silver_data,how='outer',on='date') # get date range from 2000 until today and fill gaps
    silver_data = silver_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    silver_data['type'] = 'new'
    hist['type'] = 'history'
    silver = pd.concat([silver_data,hist])
    silver[silver.duplicated(subset=['date'],keep=False)] = silver[(silver.duplicated(subset=['date'],keep=False))&(silver['type']=='new')]
    silver = silver[silver['date'].isna()==False]
    silver.drop(columns='type',inplace=True)
    silver = silver[silver['date']>='2000-01-01']
    silver = silver[silver['silver open'].isna()==False]
    silver = silver.sort_values('date')
    silver = silver.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "silver.csv", silver)

def create_oil(key_AV):    
    function = 'TIME_SERIES_DAILY'
    symbol = 'USO'  # Oil ETF
    try:
        hist = read_s3_file(S3_PREFIX + "oil.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    oil_data = pd.DataFrame(data)
    oil_data = pd.concat([oil_data['Meta Data'],oil_data['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    oil_data = oil_data.reset_index()
    oil_data = oil_data.iloc[5:]
    oil_data.drop(columns=['Meta Data',0],inplace=True)
    oil_data.rename(columns={'index':'date'},inplace=True)
    oil_data.columns = [f'oil{col[2:]}' if i >= 1 else col for i, col in enumerate(oil_data.columns)]
    oil_data['date'] = pd.to_datetime(oil_data['date'],format='%Y-%m-%d')
    oil_data['oil open'] = pd.to_numeric(oil_data['oil open'], errors='coerce')
    oil_data['oil high'] = pd.to_numeric(oil_data['oil high'], errors='coerce')
    oil_data['oil low'] = pd.to_numeric(oil_data['oil low'], errors='coerce')
    oil_data['oil close'] = pd.to_numeric(oil_data['oil close'], errors='coerce')
    oil_data['oil volume'] = pd.to_numeric(oil_data['oil volume'], errors='coerce')
    oil_data['oil high-low'] = oil_data['oil high'] - oil_data['oil low']
    oil_data = oil_data.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    oil_data = date_range.merge(oil_data,how='outer',on='date') # get date range from 2000 until today and fill gaps
    oil_data = oil_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    oil_data['type'] = 'new'
    hist['type'] = 'history'
    oil = pd.concat([oil_data,hist])
    oil[oil.duplicated(subset=['date'],keep=False)] = oil[(oil.duplicated(subset=['date'],keep=False))&(oil['type']=='new')]
    oil = oil[oil['date'].isna()==False]
    oil.drop(columns='type',inplace=True)
    oil = oil[oil['date']>='2000-01-01']
    oil = oil[oil['oil open'].isna()==False]
    oil = oil.sort_values('date')
    oil = oil.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "oil.csv", oil)

def create_platinum(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'PPLT'  # platinum 
    try:
        hist = read_s3_file(S3_PREFIX + "platinum.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    platinum_data = pd.DataFrame(data)
    platinum_data = pd.concat([platinum_data['Meta Data'],platinum_data['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    platinum_data = platinum_data.reset_index()
    platinum_data = platinum_data.iloc[5:]
    platinum_data.drop(columns=['Meta Data',0],inplace=True)
    platinum_data.rename(columns={'index':'date'},inplace=True)
    platinum_data.columns = [f'platinum{col[2:]}' if i >= 1 else col for i, col in enumerate(platinum_data.columns)]
    platinum_data['date'] = pd.to_datetime(platinum_data['date'],format='%Y-%m-%d')
    platinum_data['platinum open'] = pd.to_numeric(platinum_data['platinum open'], errors='coerce')
    platinum_data['platinum high'] = pd.to_numeric(platinum_data['platinum high'], errors='coerce')
    platinum_data['platinum low'] = pd.to_numeric(platinum_data['platinum low'], errors='coerce')
    platinum_data['platinum close'] = pd.to_numeric(platinum_data['platinum close'], errors='coerce')
    platinum_data['platinum volume'] = pd.to_numeric(platinum_data['platinum volume'], errors='coerce')
    platinum_data['platinum high-low'] = platinum_data['platinum high'] - platinum_data['platinum low']
    platinum_data = platinum_data.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    platinum_data = date_range.merge(platinum_data,how='outer',on='date') # get date range from 2000 until today and fill gaps
    platinum_data = platinum_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    platinum_data['type'] = 'new'
    hist['type'] = 'history'
    platinum = pd.concat([platinum_data,hist])
    platinum[platinum.duplicated(subset=['date'],keep=False)] = platinum[(platinum.duplicated(subset=['date'],keep=False))&(platinum['type']=='new')]
    platinum = platinum[platinum['date'].isna()==False]
    platinum.drop(columns='type',inplace=True)
    platinum = platinum[platinum['date']>='2000-01-01']
    platinum = platinum[platinum['platinum open'].isna()==False]
    platinum = platinum.sort_values('date')
    platinum = platinum.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "platinum.csv", platinum)

def create_palladium(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'PALL'  # palladium 
    try:
        hist = read_s3_file(S3_PREFIX + "palladium.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    response = requests.get(url)
    data = response.json()
    palladium_data = pd.DataFrame(data)
    palladium_data = pd.concat([palladium_data['Meta Data'],palladium_data['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    palladium_data = palladium_data.reset_index()
    palladium_data = palladium_data.iloc[5:]
    palladium_data.drop(columns=['Meta Data',0],inplace=True)
    palladium_data.rename(columns={'index':'date'},inplace=True)
    palladium_data.columns = [f'palladium{col[2:]}' if i >= 1 else col for i, col in enumerate(palladium_data.columns)]
    palladium_data['date'] = pd.to_datetime(palladium_data['date'],format='%Y-%m-%d')
    palladium_data['palladium open'] = pd.to_numeric(palladium_data['palladium open'], errors='coerce')
    palladium_data['palladium high'] = pd.to_numeric(palladium_data['palladium high'], errors='coerce')
    palladium_data['palladium low'] = pd.to_numeric(palladium_data['palladium low'], errors='coerce')
    palladium_data['palladium close'] = pd.to_numeric(palladium_data['palladium close'], errors='coerce')
    palladium_data['palladium volume'] = pd.to_numeric(palladium_data['palladium volume'], errors='coerce')
    palladium_data['palladium high-low'] = palladium_data['palladium high'] - palladium_data['palladium low']
    palladium_data = palladium_data.sort_values('date')
    # create date range from 2000 until today and fill gaps
    end_date = datetime.today().strftime('%Y-%m-%d') 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    palladium_data = date_range.merge(palladium_data,how='outer',on='date') # get date range from 2000 until today and fill gaps
    palladium_data = palladium_data.ffill()
    # to keep most recent available values instead of historic ones when removing duplicates
    palladium_data['type'] = 'new'
    hist['type'] = 'history'
    palladium = pd.concat([palladium_data,hist])
    palladium[palladium.duplicated(subset=['date'],keep=False)] = palladium[(palladium.duplicated(subset=['date'],keep=False))&(palladium['type']=='new')]
    palladium = palladium[palladium['date'].isna()==False]
    palladium.drop(columns='type',inplace=True)
    palladium = palladium[palladium['date']>='2000-01-01']
    palladium = palladium[palladium['palladium open'].isna()==False]
    palladium = palladium.sort_values('date')
    palladium = palladium.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "palladium.csv", palladium)

# ### Gold
def create_gold(key_AV):
    function = 'TIME_SERIES_DAILY'
    symbol = 'GLD'  # Gold
    try:
        hist = read_s3_file(S3_PREFIX + "gold.csv")
        hist['date'] = pd.to_datetime(hist['date'],format='%Y-%m-%d')
        start_date = hist['date'].iloc[-10]
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}'
    # if the dataframe does not exist yet, we extract all the data
    except Exception as e: 
        start_date = '2000-01-01'
        hist = pd.DataFrame()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={key_AV}&outputsize=full'
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    response = requests.get(url)
    data = response.json()
    gold_data = pd.DataFrame(data)
    gold_data = pd.concat([gold_data['Meta Data'],gold_data['Time Series (Daily)'].apply(lambda x: pd.Series(x))],axis=1)
    gold_data = gold_data.reset_index()
    gold_data = gold_data.iloc[5:]
    gold_data.drop(columns=['Meta Data',0],inplace=True)
    gold_data.rename(columns={'index':'date'},inplace=True)
    gold_data.columns = [f'gold{col[2:]}' if i >= 1 else col for i, col in enumerate(gold_data.columns)]
    gold_data['date'] = pd.to_datetime(gold_data['date'],format='%Y-%m-%d')
    gold_data = gold_data.sort_values('date').reset_index(drop=True)
    gold_data = gold_data[['date','gold open']]
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range = pd.DataFrame({'date':date_range})
    date_range['date'] = pd.to_datetime(date_range['date'],format='%Y-%m-%d')
    gold_data = date_range.merge(gold_data,how='outer',on='date')
    gold_data = gold_data.ffill()
    gold_data = gold_data[gold_data['gold open'].isna()==False]
    gold_data = gold_data.sort_values('date').reset_index(drop=True)
    # to keep most recent available values instead of historic ones when removing duplicates
    gold_data['type'] = 'new'
    hist['type'] = 'history'
    gold = pd.concat([gold_data,hist])
    gold[gold.duplicated(subset=['date'],keep=False)] = gold[(gold.duplicated(subset=['date'],keep=False))&(gold['type']=='new')]
    gold = gold[gold['date'].isna()==False]
    gold.drop(columns='type',inplace=True)
    gold = gold.sort_values(by='date')
    gold = gold.reset_index(drop=True)
    write_s3_file(S3_PREFIX + "gold.csv", gold)

# Lambda handler
def lambda_handler(event, context):
    key_FRED = os.getenv('key_FRED')
    key_AV = os.getenv('key_AV')
    
    try:
        create_us_rates(key_AV)
    except Exception as e:
        print(f'Error getting rates: {e}')
    try:
        create_snp(key_AV)
    except Exception as e:
        print(f'Error getting snp: {e}')
    try:
        create_nasdaq(key_AV)
    except Exception as e:
        print(f'Error getting nasdaq: {e}')
    try:
        create_cpi(key_FRED)
    except Exception as e:
        print(f'Error getting cpi: {e}')
    try:
        create_usd_chf(key_FRED)
    except Exception as e:
        print(f'Error getting usd_chf: {e}')
    try:
        create_eur_usd(key_FRED)
    except Exception as e:
        print(f'Error getting eur_usd: {e}')
    try:
        create_gdp(key_FRED)
    except Exception as e:
        print(f'Error getting gdp: {e}')
    try:
        create_silver(key_AV)
    except Exception as e:
        print(f'Error getting silver: {e}')
    try:
        create_oil(key_AV)
    except Exception as e:
        print(f'Error getting oil: {e}')
    try:
        create_platinum(key_AV)
    except Exception as e:
        print(f'Error getting platinum: {e}')
    try:
        create_palladium(key_AV)
    except Exception as e:
        print(f'Error getting palladium: {e}')
    try:
        create_gold(key_AV)
    except Exception as e:
        print(f'Error getting gold: {e}')
        
        
    # Send an event to EventBridge
    eventbridge = boto3.client('events')
    response = eventbridge.put_events(
        Entries=[
            {
                'Source': 'data_extraction',  
                'DetailType': 'data_extraction_done',  
                'Detail': json.dumps({'status': 'success'}),
                'EventBusName': 'default'  
            }
        ]
    )        
        
        
    return {
        'statusCode': 200,
        'body': 'Data extracted successfully'
    }

response = lambda_handler({}, {})
print(response)
