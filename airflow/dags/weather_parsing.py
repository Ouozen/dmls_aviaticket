import csv
import codecs
import urllib.request

from datetime import date, datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task


base_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
api_key = 'BLE78D5FEEXVKV2Z69PXB77SV'
unit_group ='us' #metric system
location = "O'Hare,Chicago"
content_type = 'csv'
include = 'hours'

today = datetime.strftime(date.today(), '%Y-%m-%d')

default_args = {
    'owner': 'yakunin',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 12),
    'schedule_interval': '* * * * *'
}


@dag(default_args=default_args, catchup=False)
def weather_parsing():
    def form_api_query(date, base_url, api_key, unit_group, location, include):
        """Combine API query for visualcrossing

        :param date: date of weather (exact or prediction)
        :param base_url: visualcrossing page address
        :param api_key: api key for access
        :param unit_group: unit of data
        :param location: city and state
        :param include: hours or days accuracy
        :return: string of query
        """
        api_query = base_url + location + '/' + date + \
                '?' + '&unitGroup=' + unit_group + '&contentType=' + \
                content_type + '&include=' + include + '&key=' + api_key
        return api_query


    def get_weather_info_via_api(date, base_url, api_key, unit_group, location, include):
        '''Gets weather data from visualcrossing and creates DataFrame

        :param date: date of weather (exact or prediction)
        :param base_url: visualcrossing page address
        :param api_key: api key for access
        :param unit_group: unit of data
        :param location: city and state
        :param include: hours or days accuracy
        :return: string of query
        '''
        api_query = form_api_query(date, base_url, api_key, unit_group, location, include)
        
        csv_bytes = urllib.request.urlopen(api_query)
        csv_text = csv.reader(codecs.iterdecode(csv_bytes, 'utf-8'))
        
        columns = next(csv_text)
        weather = pd.DataFrame(csv_text, columns=columns).drop(columns=['name', 'stations'])
        return weather


    @task(retries=3)
    def save_weather(date, base_url, api_key, unit_group, location, include, file_name=None):
        ''' Saves DataFrame with weather info

        :param date: date of weather (exact or prediction)
        :param base_url: visualcrossing page address
        :param api_key: api key for access
        :param unit_group: unit of data
        :param location: city and state
        :param include: hours or days accuracy
        :return: string of query
        '''
        weather = get_weather_info_via_api(date, base_url, api_key, unit_group, location, include)
        if file_name is None:
            file_name = f'weather_{date}.csv'
        weather.to_csv(file_name)

    
    save_weather(today, base_url, api_key, unit_group, location, include)


weather_parsing = weather_parsing()