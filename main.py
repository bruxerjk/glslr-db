"""
Script to retrieve data and fill sqllite db

@author: jacobb
"""
import sqlite3
import pandas as pd

from datetime import datetime, timedelta

from fetchers import chs
from fetchers import noaa


def table_exists(con, table_name):

    c = con.cursor()
    # c.execute("SELECT count(name) FROM sqlite_master WHERE TYPE = 'table' AND name = ? ", table_name) 
    qry = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
    if c.execute(qry, (table_name,)).fetchone() is not None:
        return True 
    else:
        return False


def create_table(con, table_name):

    c = con.cursor()
    
    if not table_exists(table_name): 
        c.execute("CREATE TABLE table_name(datetime DATE, value REAL)")
        return 1
    else:
        return 0


def fetch_stn_data(stn, start, end):
    
    if stn['provider'] == 'CHS':
        
        df=None
        df = chs.fetch_chs_levels(stn['id'], 
                                  start, end,
                                  timestep='hourly',
                                  cd=stn['cd'])

    elif stn['provider'] == 'NOAA':
        df=None
        df = noaa.fetch_noaa_levels(stn['id'],
                                    start, end,
                                    timestep='hourly')
    return df


def get_datatable(con, table_name, start=None, end=None):
    
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


def stns_data_to_db(con, stations, start, end):
    
    for ix, stn in stations.iterrows():

        name = stn['name']
        print(f"Station {str(ix+1)} of {len(stations)}: fetch data for {name}")
        
        df = fetch_stn_data(stn, start, end)

        if df is not None:
            df.to_sql(str(stn['id']), con, if_exists='append', index=True)
   
    return 1
    

def stns_info_to_db(filename='stations.csv'):
    
    stations = pd.read_csv(filename, dtype={'id':str})
    
    stations.set_index('id').to_sql('stations', con, if_exists='append', index=True)
    
    return 1



if __name__ == '__main__':
    
    # connection to db
    con = sqlite3.connect('glslr.db')
    
    # parameters
    end = datetime.now()
    start = datetime.today()-timedelta(days=3)
    
    # fetch data and populate database
    stations = get_datatable(con, 'stations')
    stns_data_to_db(con, stations, start, end)
    
    # # Get data for specific stations
    # df = get_datatable(con, '10050', start)
    # print(df.head())
    # df = get_datatable(con, '9099064', start, end)
    # print(df.head())
    con.close()
   
