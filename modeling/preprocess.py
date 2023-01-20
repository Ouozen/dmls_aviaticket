import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder

import datetime
import warnings
warnings.filterwarnings('ignore')

def convert_negative_delay(train):
    train['delay'] = np.maximum(train['delay'].to_numpy(), 0)
    
def drop_columns(data, columns):
    return data.drop(columns, axis=1)

def categorize_windspeed(data, bound):
    data[f'winspeed_greater_{bound}'] = (data['windspeed'] > bound) + 0
    
def categorize_preciptype(data):
    data['is_freezing_rain'] = (data['preciptype'] == 'freezingrain') + 0
    
def categorize_time(data):
    data['is_first_day_half'] = \
        (data['scheduled_datetime'] > datetime.time(6, 0)) & \
        (data['scheduled_datetime'] < datetime.time(18, 0))
    
def get_season(date):
    day_of_year = date.timetuple().tm_yday
    spring = range(80, 172)
    summer = range(172, 264)
    fall = range(264, 355)

    if day_of_year in spring:
        return 0
    elif day_of_year in summer:
        return 1
    elif day_of_year in fall:
        return 2
    return 3

def categorize_date(data):
    data['weekday'] = data['date'].apply(lambda x: x.weekday())
    data['is_weekend'] = data['weekday'] >= 5
    data['season'] = data['date'].apply(get_season)
    
def categorize(data):
    categorize_windspeed(data, 17)
    categorize_preciptype(data)
    categorize_time(data)
    categorize_date(data)
    
def change_null(data, columns, values):
    for column, value in zip(columns, values):
        data[column].fillna(value, inplace=True)

def label_encode(data, columns):
    data[columns] = data[columns].apply(LabelEncoder().fit_transform)
    
    
def preprocess(data, is_data_train=False, needs_labeling=True):
    if is_data_train:
        convert_negative_delay(data)
        
    columns_to_drop = [
        'feelslike', 'dew', 'precipprob', 'solarenergy', 'uvindex',
        'icon', 'snow', 'severerisk', 'datetime_round', 'datetime']
    data = drop_columns(data, columns_to_drop)
    categorize(data)
    change_null(data, ['windgust', 'solarradiation', 'preciptype'], [0, 0, 'no rain'])
    if needs_labeling:
        label_encode(data, ['carrier_code', 'destination_airport', 'preciptype', 'conditions'])
    return data
