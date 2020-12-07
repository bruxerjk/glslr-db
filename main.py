"""
Script to retrieve Great Lakes - St. Lawrence River water level data
and fill a sqllite database

@author: Jacob Bruxer
"""

import logging
import sqlite3
import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path

from fetchers import chs
from fetchers import noaa


path = Path(__file__).parent.resolve()

LOG_PATH = path / 'log.log'
DB_PATH = path / 'glslr.db'
STATIONS_PATH = path / 'stations.csv'

def logger_setup():
    """
    Setup program logging

    Returns
    -------
    logger : logger object
        Logger object has both console and file handlers        

    """
        
    # Create a custom logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Create console and file handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(LOG_PATH)
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.WARNING)
    
    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    logger.info('test')
    logger.warning('test2')
        
    return logger


def table_exists(con, table_name):
    """ Check if table exists in database 
    
        Returns True if table_name exists, otherwise False"""
    
    c = con.cursor()
    qry = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
    
    if c.execute(qry, (table_name,)).fetchone() is not None:
        return True 
    else:
        return False


def create_table(con, table_name):
    """ Creates table if table doesn't exist
    
        Returns True if table_name exists, otherwise False"""
        
    c = con.cursor()
    
    if not table_exists(table_name): 
        c.execute("CREATE TABLE ?(datetime DATE, value REAL)", (table_name))
        
    return table_exists(table_name)
    

def stns_info_to_db(filename):
    """
    Reads csv station info and inserts to database    

    Parameters
    ----------
    filename : csv file of station info

    Returns
    -------
    Dataframe of station info if successful, False on error

    """
        
    try:
        stations = pd.read_csv(filename, dtype={'id':str}) # read 'id' as str, not int
        
        stations.set_index('id').to_sql('stations', con, if_exists='append', index=True)
        
        return stations
    
    except:
        logger.error("Error saving stations to database")
        return False


def fetch_stn_data(stn, start, end):
    """
    Fetches individual station data from date/time start to date/time end

    Parameters
    ----------
    stn : dict
        Key 'provider' is used to determine correct fetcher (CHS or NOAA)
        Key 'id' is used to fetch correct station from provider.
    start, end : datetime
        Start and end of fetch period.

    Returns
    -------
    df : dataframe
        Fetched data table with columns datetime, value.

    """
    df = None
    
    if stn['provider'] == 'CHS':
        
        df = chs.fetch_chs_levels(stn['id'], 
                                  start, end,
                                  timestep='hourly',
                                  cd=stn['cd'])  # to convert from cd to IGLD

    elif stn['provider'] == 'NOAA':

        df = noaa.fetch_noaa_levels(stn['id'],
                                    start, end,
                                    timestep='hourly')
        
    if df is not None:
        return df

    else:
        logger.warning(f"Failed to fetch data for {stn['name']}")
        return None


def stns_data_to_db(con, stations, start, end):
    """
    Fetches and stores data from multiple stations

    Parameters
    ----------
    con : database connection

    stations : dataframe
        Contains rows of station info.
    start, end : datetime
        Start and end of fetch period.

    """
        
    for ix, stn in stations.iterrows():

        name = stn['name']
        logger.info(f"Station {str(ix+1)} of {len(stations)}: fetch data for {name}")
        
        df = fetch_stn_data(stn, start, end)

        if df is not None:
            df.to_sql(str(stn['id']), con, if_exists='append', index=True)



def get_datatable(con, table_name, start=None, end=None):
    """
    Gets a table of data from database

    Parameters
    ----------
    con : database connection

    table_name : string
        Name of table to get.
    start, end : datetime
        Start and end of fetch period. Default(s) = None, returns full table

    Returns
    -------
    df : dataframe
        Table of data from database if succesful; else returns None.

    """
    
    qry = "SELECT * FROM '{}'"
    
    params = []
    
    # check table exists 
    if table_exists(con, table_name):
        
        if start:
            qry = qry + " WHERE datetime >= ?"
            params.append(start)
            
            if end:
                qry = qry + " AND datetime <= ?"
                params.append(end)     
                
        elif end:
            
            qry = qry + " WHERE datetime <= ?"
            params.append(end)
            
        qry += ";"
    
        df = pd.read_sql_query(qry.format(table_name), con, params=params)
        
        return df
        
    else:
        
        return None
    
    
if __name__ == '__main__':
    
    logger = logger_setup()
    
    # connection to db
    logger.info('Connecting to database')
    try:
        con = sqlite3.connect(DB_PATH)
    except Exception as e:
        logger.error('Connection to database failed', exc_info=True)
        
    # parameters
    end = datetime.now()
    start = datetime.today()-timedelta(days=3)

    # add station info if doesn't exist, also builds db if doesn't exist
    if table_exists(con, 'stations') is False:
        stations = stns_info_to_db(STATIONS_PATH)
    else:
        stations = get_datatable(con, 'stations') 
    
    # fetch data and populate database       
    stns_data_to_db(con, stations, start, end)
    
    # # Get data for specific stations
    # df = get_datatable(con, '10050', start)

    con.close()
