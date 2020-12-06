"""
Created on Wed Apr 22 16:11:33 2020

@author: jacobb
"""
import pandas as pd
from numpy import around
from zeep import Client
from datetime import date, datetime, timedelta

def resample_noaa(df, timestep='default'):
    
    if timestep in ['hourly', 'daily']:
        
        # resample to hourly
        df = df.resample('1H').first()
  
        # compute daily from hourly
        if timestep == 'daily':
                       
            # shift hourly back 1 hour so that hour 24 gets included in 
            # mean for previous day (NOAA standard practice)
            df = around(df.shift(-1).groupby(df.index.date).mean(),2)
            
            # drop today's date since not complete
            # (if end is earlier it doesn't matter)
            today = date.today()
            yesterday = today + timedelta(days=-1)
       
            df = df.loc[:yesterday]
            
    else:
        
        print('Returning default timestep = 6 minutes')
    
    return df


def fetch_noaa_levels(stn_ID, start, end=datetime.now(), timestep='default'):
    '''
    Fetches data for a NOAA water level station 
    from web services

    Parameters
    ----------
    stn_ID : str
        7-digit NOAA station ID (e.g., 9052030).
    start : datetime object
        Starting date of fetched data.
    end : datetime object, optional
        Ending date of fetched object. The default is datetime.now().
    timestep : TYPE, optional
        Timestep of data returned.  Options are "daily", "hourly" or 
        else 6-minute is returned (the NOAA default). 
        The function default is 'hourly' since this is the most 
        commonly used value.

    Returns
    -------
    df : pandas dataframe
        Dataframe table of NOAA dates/times and water levels.

    '''    
   
    # NOAA data fetched from SOAP
    
    NOAA_WSDL = 'https://opendap.co-ops.nos.noaa.gov/axis/webservices/waterlevelrawsixmin/wsdl/WaterLevelRawSixMin.wsdl'
    soap_client = Client(NOAA_WSDL)

    df = pd.DataFrame(columns=['datetime', stn_ID])
    df = df.set_index('datetime')
    
    max_number_of_days = 30
       
    params = dict()
    params['stationId'] = stn_ID
    params['datum'] = 'IGLD'
    params['unit'] = 0 # 0 maps to meters, 1 maps to feet
    params['timeZone'] = 1 # 0 maps to UTC, 1 maps to LST... Antoine had 0, need to check
    

    while start < end:
        
        fetch_end = min(start+timedelta(days=max_number_of_days),
                         end)

        params['beginDate'] = start.strftime('%Y%m%d')
        params['endDate'] = fetch_end.strftime('%Y%m%d')
        
        data = soap_client.service.getWaterLevelRawSixMin(**params)
        
        start = fetch_end
        
        for water_levels in data:

            observed_lst = datetime.strptime(water_levels['timeStamp'],
                                             '%Y-%m-%d %H:%M:00.0')

            water_level=water_levels['WL']
            
            # need to convert columns from object to float
            # ... not sure why they're object to begin with           
            df.at[observed_lst, stn_ID] = pd.to_numeric(water_level)
    
    df = resample_noaa(df, timestep=timestep)
                
    return df


# from lake_station_info import tole as stn
# from lake_station_info import eri as lake

# print(stn)

# end = datetime.now()

# start = datetime(2019,1,1)
# start = datetime.today() + timedelta(days=-7)

# levels = fetch_noaa_levels(stn.stn_ID, start, end, 'daily')



# print(levels)
