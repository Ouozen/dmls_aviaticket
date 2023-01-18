import pandas as pd
from datetime import datetime

from tqdm import tqdm
from time import sleep

from bs4 import BeautifulSoup
import requests

import warnings
warnings.filterwarnings('ignore')

master_url = 'https://www.airport-ohare.com'

url_yesterday_0_6 = 'https://www.airport-ohare.com/departures.php?tp=0&day=yesterday'
url_yesterday_6_12 = 'https://www.airport-ohare.com/departures.php?tp=6&day=yesterday'
url_yesterday_12_18 = 'https://www.airport-ohare.com/departures.php?tp=12&day=yesterday'
url_yesterday_18_0 = 'https://www.airport-ohare.com/departures.php?tp=18&day=yesterday'

urls_yesterday = [url_yesterday_0_6, url_yesterday_6_12, url_yesterday_12_18, url_yesterday_18_0]

url_tomorrow_0_6 = 'https://www.airport-ohare.com/departures.php?tp=0&day=tomorrow'
url_tomorrow_6_12 = 'https://www.airport-ohare.com/departures.php?tp=6&day=tomorrow'
url_tomorrow_12_18 = 'https://www.airport-ohare.com/departures.php?tp=12&day=tomorrow'
url_tomorrow_18_0 = 'https://www.airport-ohare.com/departures.php?tp=18&day=tomorrow'

urls_tomorrow = [url_tomorrow_0_6, url_tomorrow_6_12, url_tomorrow_12_18, url_tomorrow_18_0]


def open_page(url):
    ''' Opens page for parsing with beautiful soup
    :param url: page's url
    :return: soup object
    '''
    page = requests.get(url)
    page = BeautifulSoup(page.content, 'html.parser')
    return page


def filter_clickable_items(items):
    ''' Gets links from html objects

    :param items: list of html parsed items
    :return: list of links from <items>
    '''
    clickables = []
    for flight in items:
        href = flight.find('a') # link indicator
        if href is not None:
            clickables.append(href['href'])
    return clickables


def get_flights_from_main_pages(urls):
    ''' Gets links from html objects

    :param urls: start pages to parse
    :return: list of flight pages
    '''
    flight_urls = []
    for url in urls:
        page = open_page(url)

        # getting all suitable objects from the page
        flights = page.find_all('div', {'class': 'flight-col flight-col__flight'})
        flight_urls.extend(filter_clickable_items(flights))
    return flight_urls


def format_actual_time(time, date):
    ''' Formats parsed string of actual time of flight to datetime

    :param time: string from html parser
    :param date: date of flight (string)
    :return: datetime object
    '''
    actual_time = time.text.strip()
    if actual_time[-1] != ')': # there is a date in text
        actual_time += f' ({date})'
    actual_datetime = datetime.strptime(actual_time, "%I:%M %p (%Y-%m-%d)")
    return actual_datetime


def format_scheduled_time(time, date):
    ''' Formats parsed string (12-hours format) of scheduled 
    time of flight to datetime and 24-format string

    :param time: string from html parser
    :param date: date of flight (string)
    :return: (datetime object, time string in 24-hours format)
    '''
    scheduled_time = time.text.strip().split()
    scheduled_time = scheduled_time[-2] + ' ' + scheduled_time[-1] + ' ' + date
    scheduled_datetime = datetime.strptime(scheduled_time, "%I:%M %p %Y-%m-%d")
    scheduled_time_24 = datetime.strftime(scheduled_datetime, "%H:%M")
    return scheduled_datetime, scheduled_time_24


def parse_general_items(page, url):
    ''' Parses items that are shared among test and train datasets

    :param page: page opened via beautiful soup
    :param url: corresponding url
    :return: basic info about the flight
    '''
    date = page.find('div', {'class': 'flight-info__date'}).text.strip() # %Y-%m-%d
    flight = url.split('/')[-1].split('?')[0] # XX****
    destination = page.find_all('div', {'class': 'flight-info__city'})[-1].text.strip().split()[-1][1:-1] # XXX
    scheduled_time = page.find('div', {'class': 'flight-info__sch-departed'})
    scheduled_datetime, scheduled_time = format_scheduled_time(scheduled_time, date)
    return date, flight, destination, scheduled_datetime, scheduled_time


def parse_flight_page(url):
    ''' Gets links from html objects

    :param url: link to flight page
    :return: list of flight pages
    '''
    page = open_page(url)
    date, flight, destination, scheduled_datetime, scheduled_time = parse_general_items(page, url)
    
    row = {
        'Date': date,
        'Flight': flight[2:],
        'Carrier Code': flight[:2],
        'Scheduled Time': scheduled_time,
        'Destination airport': destination
    }
    
    actual_time =  page.find('div', {'class': 'flight-info__infobox-text'})
    actual_datetime = format_actual_time(actual_time, date)
    
    delay = actual_datetime - scheduled_datetime
    delay = int(delay.total_seconds() / 60)
    status = page.find('div', {'class': 'flight-info__infobox-title'}).text.strip()
    if status != 'Departed at:': # => there is no info
        delay = None
    row['Delay'] = delay

    return row


def get_flights(urls):
    ''' Builds DataFrame with flights info

    :param urls: links of main pages with timetables
    :return: DataFrame with the following info ->
        <'Date', 'Flight', 'Carrier Code', 'Scheduled Time', 'Delay', 'Destination airport'>
    '''
    flight_urls = get_flights_from_main_pages(urls)

    yesterday_flights = pd.DataFrame(columns=[
        'Date', 'Flight', 'Carrier Code', 'Scheduled Time', 'Delay', 'Destination airport'
    ])

    for url in tqdm(flight_urls):
        flight_url = master_url + url
        row = parse_flight_page(flight_url)
        if row is None:
            continue
        yesterday_flights = yesterday_flights.append(row, ignore_index=True)
    return yesterday_flights


def save_flights(urls, file_name=None):
    ''' Saves to system DataFrame with flights info

    :param urls: links of main pages with timetables
    :param file_name: address of saved file
    '''
    yesterday_flights = get_flights(urls)
    date = yesterday_flights['Date'][0]
    if file_name is None:
        file_name = f'flights_{date}.csv'
    yesterday_flights.to_csv(file_name)

def main():
    save_flights(urls_yesterday)
    save_flights(urls_tomorrow)


if __name__ == '__main__':
    main()
