import pandas as pd
import numpy as np
import os
from datetime import datetime
import xgboost as xgb
from sklearn.metrics import accuracy_score
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import cross_val_score
import string
import boto3
from io import StringIO
import json


# AWS S3 configuration
S3_BUCKET = 'financial-project-1'
S3_PREFIX_1 = 'extraction-staging/'
S3_PREFIX_2 = 'complete-dataset/'
S3_PREFIX_3 = 'model-results/'

def read_s3_file(key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj['Body'])

def write_s3_file(key, df):
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())

# Read csv files from S3
def read_and_assign_datasets():
    file_names = [
        'usd_chf.csv', 'cpi.csv', 'us_rates.csv', 'nasdaq.csv', 'snp.csv', 
        'eur_usd.csv', 'gdp.csv', 'silver.csv', 'oil.csv', 'platinum.csv', 
        'palladium.csv', 'gold.csv'
    ]
    for file_name in file_names:
        # Create a variable name by stripping '.csv' from the file name
        dataset_name = file_name.split('.')[0]
        # Read the CSV file
        df = read_s3_file(S3_PREFIX_1 + file_name)
        # Assign the DataFrame to a variable with the name of the dataset
        globals()[dataset_name] = df

# Dataframe preperation
# We will use gold's opening price's 30 day exponential moving average as a feature 
def EMA30(gold):
    gold['gold EMA_30'] = gold['gold open'].ewm(span=30).mean()
    return gold

# dfs start at different dates. find which one has the most recent start date and trim the dataframes
def most_recent_start_date(*dfs):
    most_recent_date = None
    for df in dfs:
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')
            start_date = df.iloc[0]['date'] 
            if most_recent_date is None or start_date > most_recent_date:
                most_recent_date = start_date
    # Trim the dataframes based on the most recent start date
    trimmed_dfs = []
    for df in dfs:
        trimmed_df = df[df['date'] >= most_recent_date]  
        trimmed_dfs.append(trimmed_df) 
    # Merge all dataframes on the 'date' column
    if trimmed_dfs:
        merged_df = trimmed_dfs[0]
        for df in trimmed_dfs[1:]:
            merged_df = pd.merge(merged_df, df, on='date', how='inner')
        merged_df = merged_df.drop_duplicates(subset='date', keep='last')
        return merged_df
    else:
        return pd.DataFrame() 
    
def model_dataset(model, S3_PREFIX_2):
    # Set index as date index since we are working with a time series dataframe 
    model.set_index('date', inplace=True)
    model = model.loc[:, ~model.columns.str.contains('high|low|close|volume', regex=True)]
    # Remove weekends
    model['is_weekend'] = model.index.weekday >= 5
    model = model[model['is_weekend']==False]
    model.drop(columns='is_weekend',inplace=True)
    model = model[[col for col in model.columns if col != 'gold open'] + ['gold open']]
    write_s3_file(S3_PREFIX_2 + "model_dataset.csv", model)
    return model

def data_limits(model):
    # XGBoost does not handle extrapolation well. We need to know if we are extrapolating
    model_range = model.copy()
    model_range['max_range'] = model_range['gold open'].expanding().max()
    model_range['min_range'] = model_range['gold open'].expanding().min()
    model_range['min_range'] = model_range['min_range'].shift(1)
    model_range['max_range'] = model_range['max_range'].shift(1)
    return model_range

# XGBoost implementation
def model_implementation(model, model_range, S3_PREFIX_3):
    # Feature matrix and target vector
    X = model.iloc[:,:-1]
    y = model.iloc[:,-1:]
    # last 300 days as test set
    # Create column to divide into train and test
    X['split'] = 'train'
    mask = X.index.isin(X.index[-300:])
    X.loc[mask, 'split'] = 'test'
    y['split'] = 'train'
    mask = y.index.isin(y.index[-300:])
    y.loc[mask, 'split'] = 'test'
    X_train = X[X['split']=='train']
    X_test = X[X['split']=='test']
    y_train = y[y['split']=='train']
    y_test = y[y['split']=='test']
    X_train.drop(columns='split',inplace=True)
    X_test.drop(columns='split',inplace=True)
    y_test.drop(columns='split',inplace=True)
    y_train.drop(columns='split',inplace=True)
    # Scale the features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    # Grid Search results
    param_grid = {
        'max_depth': [2],  
        'learning_rate': [0.1],  
        'n_estimators': [100],  
        'subsample': [0.7],  
        'colsample_bytree': [0.9],  
        'colsample_bylevel': [0.9],  
        'min_child_weight': [1],  
        'reg_alpha': [0.1],  
        'reg_lambda': [0.5],  
    }
    xgb_model = XGBRegressor(objective='reg:squarederror')
    splits = 11
    # Initialize TimeSeriesSplit with the number of splits
    tscv = TimeSeriesSplit(n_splits=splits) 
    grid_search = GridSearchCV(estimator=xgb_model, param_grid=param_grid, 
                            scoring='neg_root_mean_squared_error', cv=tscv, verbose=1, n_jobs=-1)
    grid_search.fit(X_train_scaled, y_train)
    print("Best reg parameters found: ", grid_search.best_params_)
    best_model = grid_search.best_estimator_
    y_pred_test = best_model.predict(X_test_scaled)
    test_df = X_test.copy()
    test_df['gold open'] = y_test
    test_df['gold open pred'] = y_pred_test
    # get first test row
    first_test_row = test_df.index[0] 
    # check what were the limits before the test set to avoid bad results in case of extrapolation
    model_range_limits = model_range[model_range.index == first_test_row]
    lower_limit = model_range_limits['min_range'][0]
    higher_limit = model_range_limits['max_range'][0]
    test_df['min_range'] = lower_limit
    test_df['max_range'] = higher_limit
    test_df['outside_range'] = test_df.apply( lambda x: False if x['gold open']>x['min_range'] and x['gold open']<x['max_range'] else True, axis = 1)
    test_df['gold open pred'] = test_df.apply(lambda x: x['gold EMA_30'] if x['outside_range']==True else x['gold open pred'], axis = 1)
    # Test performance
    test2_rmse = np.sqrt(mean_squared_error(test_df[['gold open']], test_df['gold open pred']))
    # To monitor performance
    print(f'Test RMSE: {test2_rmse}')
    test_df.drop(columns={'min_range', 'max_range', 'outside_range'}, inplace=True)
    # add error to the test dataframe
    test_df['Error'] = test_df['gold open'] - test_df['gold open pred']
    test_df = test_df[~test_df.index.duplicated(keep='last')]
    write_s3_file(S3_PREFIX_3 + "results.csv", test_df)


read_and_assign_datasets()
gold = EMA30(gold)

def lambda_handler(event, context):
    model = most_recent_start_date(snp, nasdaq, us_rates, cpi, usd_chf, eur_usd, gdp, silver, oil, platinum, palladium, gold)
    model = model_dataset(model, S3_PREFIX_2)
    model_range = data_limits(model)
    model_implementation(model, model_range, S3_PREFIX_3)

    # Initialize a Glue client
    glue_client = boto3.client('glue')
    crawler_name = 'financial-project-1-crawler'
    try:
        # Start the Glue Crawler
        response = glue_client.start_crawler(Name=crawler_name)
        return {
            'statusCode': 200,
            'body': json.dumps(f"Crawler '{crawler_name}' started; Model implemented successfully")
        }
    except Exception as e:
        print(f"Error starting the Glue Crawler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }


response = lambda_handler({}, {})
print(response)
