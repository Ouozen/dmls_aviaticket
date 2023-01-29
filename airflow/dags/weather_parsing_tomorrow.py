import csv
import codecs
import urllib.request

from datetime import date, datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
import psycopg2


base_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
api_key = 'BLE78D5FEEXVKV2Z69PXB77SV'
unit_group ='us' #metric system
location = "O'Hare,Chicago"
content_type = 'csv'
include = 'hours'

tomorrow = datetime.strftime(date.today() + timedelta(days=1), '%Y-%m-%d')

default_args = {
    'owner': 'yakunin',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 17)
    #'schedule_interval': '* * * * *'
}


@dag(default_args=default_args, catchup=False, schedule_interval="55 19 * * *")
def weather_parsing_tomorrow():
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
        return {'filename': f'weather_{date}.csv'}


    def connection():
        """Эта функция открывает подключение и возвращает коннекшн 
        и курсор для БД

        Returns:
            bool: флаг
            object: connect
            object: cursor
        """    
        try:
            conn = psycopg2.connect(
                host='postgres',
                port='5432',
                database='airflow',
                user='postgres',
                password='MYZv3Ietuflo'
            )
        except:
            return False, 0, 0

        cur = conn.cursor()

        return True, conn, cur


    @task(retries=3)
    def insert(filenames):
        filename = filenames['filename']
        with open(filename) as file:
            csv = file.readlines()

        _, conn, cur = connection()

        columns = [col for col in csv[0].replace('\n', '').split(',')[1:]]
        types = ['timestamp'] + ['decimal(5, 1)'] * 6 + ['text'] + ['decimal(5, 1)'] * 12 + ['text'] * 2
        table_name = 'weather_predict'

        table_data = []

        for data in csv[1:]:
            data = data.replace('\n', '').split(',')[1:]
            data = ['NULL' if x == '' else x for x in data]
            string = ''
            for col, type in zip(data, types):
                if col == 'NULL':
                    string += col + ', '
                elif type == 'text':
                    string += "\'" + col + "\'" + ', '
                elif type == 'timestamp':
                    string += 'TIMESTAMP ' + "\'" + col + "\'" + ', '
                else: # остается число которое зануляем если есть буквы
                    col = col if col.replace('.', '').isdigit() else '0'
                    string += col + ', '
            string = string[:-2]
            table_data.append(string)

        query = f"""insert into {table_name} ({', '.join(columns)}) \nvalues """
        for row in table_data:
            query += f'\n({row}), '
        query = query[:-2] + ';'

        cur.execute(query)
        conn.commit()
        conn.close()


    @task(retries=3)
    def truncate_wrong_date(flag):
        if flag == None:
            pass

        _, conn, cur = connection()

        cur.execute("""
            delete from weather_predict
            where datetime in (select distinct datetime from weather);
        """)

        conn.commit()
        conn.close()

    
    filename = save_weather(tomorrow, base_url, api_key, unit_group, location, include)
    flag = insert(filename)
    truncate_wrong_date(flag)


weather_parsing_tomorrow = weather_parsing_tomorrow()