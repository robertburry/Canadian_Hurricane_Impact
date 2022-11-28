import pandas as pd
import time  # Used in Metar
import datetime  # Used in Metar
from urllib.request import urlopen  # Used in Metar
import sqlite3 as sql
import numpy as np
from multiprocessing import Pool


def construct_urls(station, start_date, finish_date):
    '''
    This function allows you to construct a list of URLs for a list of stations and start dates.
    This is also written using Python 3.6 and later, did not add a checker but one could be added in
    the future to ensure you're using a newer version of Python. 

    A checker to see if airports are within IEM database could be added if needed.
    Args:
    station: a list or tuple of strings containing the 4 letter airport/station code.
    start_date: a datetime.datetime of the calendar date of the start date. Format (year,month,day)
    finish_date: a datetime.datetime of the calendar date of the end date. Format (year,month,day)
    Returns:
      A list of urls.
    '''

    if not isinstance(station, (list, tuple)):
        raise TypeError(f'Station must be a {list} or {tuple}.')

    if not all(isinstance(x, str) for x in station):
        raise TypeError(f'Station elements must be a list all being {str}')

    if start_date > datetime.datetime.now():
        raise ValueError(f'Starting date {start_date} is in the future.')

    if finish_date > datetime.datetime.now():
        raise ValueError(f'End date {finish_date} is in the future.')

    if start_date > finish_date:
        raise ValueError(
            f'Start date {start_date} is past your finish_date {finish_date}')

    try:
        startts = datetime.datetime(*start_date)
        endts = datetime.datetime(*finish_date)

        SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
        service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
        service += startts.strftime("year1=%Y&month1=%m&day1=%d&")
        service += endts.strftime("year2=%Y&month2=%m&day2=%d&")
        return [f"{service}&station={station}" for station in station]

    except Exception as exp:
        print(f"""Bad data with: {station} - using {start_date}
        and {finish_date}.\nException raised {exp}.""")


def pull_metar(url):
    '''
    This function simply returns a Pandas DataFrame for a requested URL.
    Args:
      url: takes a url string that contains a .csv file.
    Returns:
      A Pandas DataFrame.
    '''

    return pd.read_csv(url, skiprows=5)


def clean_metar(data):
    cleaned = data._convert(numeric=True)
    cleaned.apply(pd.api.types.infer_dtype)
    cleaned['tmpf'] = cleaned.apply(lambda x: (5/9)*(x['tmpf']-32), axis=1)
    cleaned['dwpf'] = cleaned.apply(lambda x: (5/9)*(x['dwpf']-32), axis=1)
    cleaned['feel'] = cleaned.apply(lambda x: (5/9)*(x['feel']-32), axis=1)
    cleaned.rename({'drct': 'wnd_dir', 'sknt': 'wnd_spd', 'tmpf': 'temp',
                   'dwpf': 'dew_pt', 'metar': 'raw_metar'}, axis=1, inplace=True)
    return cleaned


def main():
    df = pd.read_csv('eastern_canada_airports.csv')
    can_stations = df['stid'].tolist()
    hurricane_metars = pd.DataFrame()
    url = []
    for year in range(2014, 2022):
        start_date = (year, 5, 1)
        finish_date = (year, 11, 30)
        url += construct_urls(can_stations, start_date, finish_date)
    for r in pool.map(pull_metar, url):
        hurricane_metar = pd.concat([hurricane_metar, r], ignore_index=True)
    cnx = sql.connect('Canadian_Hurricane_Impact.db')
    hurricane_metar.to_sql('Metar_Data', cnx, if_exists='append', index = False)
