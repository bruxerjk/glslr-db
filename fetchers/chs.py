import pandas as pd
from numpy import around, nan
from zeep import Client
from datetime import datetime, timedelta, timezone

UTC_OFFSET = datetime.utcnow()-datetime.now()
EST_OFFSET = timedelta(hours=-5)

def convert_to_utc(dt):
    '''
    Convert datetime to UTC.

    If datetime object is "unaware" this means it doesn't have timezone info
    associated with it, so it assumes local time

    If datetime object is aware it has timezone info associated with it
    and it gets replaced with UTC

    Parameters
    ----------
    dt : datetime object
        Datetime to conver to UTC.

    Returns
    -------
    Datetime object in UTC.

    '''
    if dt.tzinfo is None: # assume local standard time

        dt = dt + UTC_OFFSET

    else:

        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def clean_CHS(timeseries):


    mean = timeseries.mean()
    stdev = timeseries.std()

    max_chg = 2.5

    try:
        # remove 3x repeated values
        cond1 = timeseries.diff(1)!=0.0
        cond2 = timeseries.diff(2)!=0.0
        timeseries = timeseries.where(cond1 & cond2, nan)

        # remove values where change exceeds +/- max
        timeseries = timeseries.where(timeseries.diff(1)<max_chg, nan)
        timeseries = timeseries.where(timeseries.diff(1)>-1*max_chg, nan)

        # should log values removed

        # compute mean and remove the really high or really low values as well

        # check what values are filtered by reversing sign
        # timeseries = timeseries.where(timeseries>mean+4*stdev)
        timeseries = timeseries.where(timeseries<mean+4*stdev)
        timeseries = timeseries.where(timeseries>mean-4*stdev)

    except:
        print('error')

    return timeseries


def resample_chs(df, timestep='default'):
    '''
    Resample CHS data to different timestep (e.g., hourly --> daily)

    Parameters
    ----------
    df : pandas dataframe
        Table of datetimes (index) and water level data to resample
    timestep : string, optional
        Timestep to resample data to (e.g., 'daily'). The default is 'default'.

    Returns
    -------
    df : pandas dataframe
        Resampled data.

    '''

    if timestep=='hourly':

        df = df.resample('1H').first()

    elif timestep == 'daily':

        # compute daily means

        # make sure enough hours in a day to count
        min_num_hrs_mask = df.resample("D").count() >= 18

        # resample to compute means
        df = df.resample("D").mean()

        # use mask to convert days with < min hrs to NaN
        df = df[min_num_hrs_mask]

        # round levels to 2 decimals
        df = around(df, 2)

        # # drop today's date since not complete
        # # (if end is earlier it doesn't matter)
        # today = date.today()
        # yesterday = today + timedelta(days=-1)

        # df = df.loc[:yesterday]
        df.index.name = 'Date'

    else:
        print('No resampling timestep indicated; returning data as-is')

    return df


def chs_wsdl():
    '''
    Establishes SOAP client for data retrieval from CHS WSDL

    Returns
    -------
    soap_client :
        wsdl client to fetch CHS data.
    station_id_list : list
        List of strings with all available stations returned from
        soap_client.  Needed since CHS doesn't have all stations in system.

    '''
    CHS_WSDL = 'https://ws-shc.qc.dfo-mpo.gc.ca/observations?wsdl'

    soap_client = Client(CHS_WSDL)

    for service in soap_client.wsdl.services.values():
        print("Connected to service:", service.name)

    x = soap_client.service.getMetadata()

    for d in x:

        if d['name'] == 'station_id_list':
            station_id_list = d['value'].split(',')

    return soap_client, station_id_list


def fetch_chs_levels(stn_ID, start, end=datetime.utcnow(), timestep='hourly', cd=None):
    '''
    Fetches data for a CHS water level station
    from web services

    Parameters
    ----------
    stn_ID : str
        5-digit CHS station ID (e.g., 15930).
    start : datetime object
        Starting date of fetched data.
    end : datetime object, optional
        Ending date of fetched object. The default is datetime.utcnow().
    timestep : str, optional
        Timestep of data returned.  Options are "daily", "hourly", "15-min" or
        else 3-minute is returned (the CHS default).
        The function default is 'hourly' since this is the most
        commonly used value.

    Returns
    -------
    df : pandas dataframe
        Dataframe table of CHS dates/times and water levels.

    '''
    try:
        assert type(stn_ID) == str
        assert len(stn_ID) == 5
    except:
        raise ValueError('CHS station ID must be string of length 5')
        
    soap_client, station_id_list = chs_wsdl()


    if stn_ID not in station_id_list:

        # check to make sure station is available, some aren't
        print(f"Station {stn_ID} not available")
        df = None

    else:

        try:

             # alternatively could use station name but this is
            metadataSelection = {#'station_name': stn.name,
                                 'station_id': stn_ID,
                                 'vl':'1+'} # validation level

            # join metadata for selection using proper syntax
            # Example: "station_id=15930::station_name=Sorel::vl=1+"
            metadataSelection = '::'.join(('{}={}').format(key,value) for key,value in metadataSelection.items())

            # all of these parameters are mandatory
            params = {'dataName': 'wl',  # water levels
                      'latitudeMin': 40, # used extent of entire GL-SL basin, then
                      'latitudeMax': 51, # metadata used to selection station
                      'longitudeMin': -93.5,
                      'longitudeMax': -69,
                      'depthMin': 0,    # used for depth of measurements (i.e., underwater)
                      'depthMax': 0,
                      'dateMin': '2020-01-01 00:00:00',  # placeholders, modified later
                      'dateMax': '2020-01-31 00:00:00',
                      'start': 1,  # start of returned data indexed at 1
                      'sizeMax': 1000,  # max is 1000
                      'metadata': True,
                      'metadataSelection': metadataSelection,
                      'order': "asc"}

            start_utc = convert_to_utc(start)

            end_utc = convert_to_utc(end)

            if timestep == 'daily':
                # if daily data is selected, the end variable might be
                # passed as a datetime object at hour 00:00, so
                # add 24 hours to ensure all hourly data are collected
                # for the end day and then this can be used to compute daily
                end_utc = end_utc + timedelta(hours=24)

            delta = timedelta(days=1)

            df = pd.DataFrame(columns=['datetime', stn_ID])
            df = df.set_index('datetime')

            while start_utc <= end_utc:

                # have to loop through days one day at a time,
                # the max number of days is just over 2
                # (since sizeMax = 1000, and 3-minute data returned),
                # so this ensures everything is retrieved, although it's slow

                params['dateMin'] = start_utc.strftime("%Y-%m-%d %H:%M:00")
                params['dateMax'] = min(end_utc,
                                        start_utc+delta).strftime("%Y-%m-%d %H:%M:00")

                print('Fetching {} data from {} to {}'.format(stn_ID,
                                                              params['dateMin'],
                                                              params['dateMax']))
                
                data = soap_client.service.search(**params)

                for water_levels in data['data']:

                    if timestep in ['hourly', 'daily']:

                        if ':00:00' in water_levels['boundaryDate']['max']:
                            # print(water_levels['metadata'][0]['value'],
                            #       water_levels['metadata'][1]['value'],
                            #       water_levels['boundaryDate']['max'],
                            #       water_levels['value'])
                            observed_utc = datetime.strptime(water_levels['boundaryDate']['max'], '%Y-%m-%d %H:%M:00')
                            # convert utc to eastern standard time

                            observed_est = observed_utc + EST_OFFSET

                            water_level=water_levels['value']

                            df.at[observed_est, stn_ID] = water_level

                    elif timestep == '15-min':

                        times = [':00:00', ':15:00', ':30:00', ':45:00']

                        if any(time in water_levels['boundaryDate']['max'] for time in times):
                            # print(water_levels['metadata'][0]['value'],
                            #       water_levels['metadata'][1]['value'],
                            #       water_levels['boundaryDate']['max'],
                            #       water_levels['value'])
                            observed_utc = datetime.strptime(water_levels['boundaryDate']['max'], '%Y-%m-%d %H:%M:00')
                            # convert utc to eastern standard time

                            observed_est = observed_utc + EST_OFFSET
                            water_level=water_levels['value']
                            df.at[observed_est, stn_ID] = water_level

                    else:  # 3-minute
                        # print(water_levels['metadata'][0]['value'],
                        #       water_levels['metadata'][1]['value'],
                        #       water_levels['boundaryDate']['max'],
                        #       water_levels['value'])
                        observed_utc = datetime.strptime(water_levels['boundaryDate']['max'], '%Y-%m-%d %H:%M:00')

                        # convert utc to eastern standard time
                        observed_est = observed_utc + EST_OFFSET
                        water_level=water_levels['value']
                        df.at[observed_est, stn_ID] = water_level

                    # need to convert columns from object to float
                    # not sure why they're object to begin with
                    df = df.apply(pd.to_numeric, errors ='coerce')

                start_utc += delta

            #df = df + cd

            # compute daily from hourly
            if timestep == 'daily':

                df = resample_chs(df, timestep = 'daily')

            df = df[start:end]

            if cd:
                df = df.rename(columns={stn_ID : 'metres'})
                df['metres'] = df['metres'] + cd
            
        except:
            print(f"Problem with station {stn_ID}; no data returned")
            df = None

    return df


# TESTING #


# from lake_station_info import sore as stn
# from lake_station_info import eri as lake


# time_start = datetime(2020,5,2)
# time_end = datetime(2020,5,4)

# levels = fetch_chs_levels(stn.stn_ID, time_start, time_end, 'hourly')
# levels = levels + 3.775


# #%%
# levels_daily = resample_chs(levels, timestep='daily')


# print(levels)
