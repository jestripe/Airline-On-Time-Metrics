import pandas as pd
import numpy as np
import zipfile
import psycopg2
import os
import string 
import sqlalchemy as sqla
import datetime
from sqlalchemy import NCHAR, create_engine
from metrics_python.functions import set_psql_auth
from metrics_python.functions import return_psql_auth

uid, pwrd = return_psql_auth()

psql_connect_string = f'''postgresql+psycopg2://{uid}:{pwrd}@localhost/airlineontimemetrics'''

psql_engine =create_engine(psql_connect_string)

path = '/home/jestripe/GradSchool/MIS_581_Winter_2021/RawData'

# code from https://www.codegrepper.com/code-examples/python/python+unzip+all+files+in+directory
def unzipFiles(path):
    files = os.listdir(path)
    for file in files:a
    if file.endswith('.zip'):
        filePath = path + '/' + file
        zip_file = zipfile.ZipFile(filePath)
        for names in zip_file.namelist():
            zip_file.extract(names,path)
        zip_file.close()


flight_data_dict = {'Year': sqla.types.INTEGER,
                    'Quarter': sqla.types.INTEGER,
                    'Month': sqla.types.INTEGER,
                    'DayofMonth': sqla.types.INTEGER,
                    'DayOfWeek': sqla.types.INTEGER,
                    'FlightDate': sqla.types.DATE,
                    'IATA_CODE_Reporting_Airline': sqla.types.CHAR(2),
                    'Tail_Number': sqla.types.CHAR(6),
                    'Flight_Number_Reporting_Airline': sqla.types.INTEGER,
                    'Origin': sqla.types.CHAR(3),
                    'Dest': sqla.types.CHAR(3),
                    'CRSDepTime': sqla.types.TIME,
                    'DepTime': sqla.types.TIME,
                    'DepDelay': sqla.types.INTEGER,
                    'DepDelayMinutes': sqla.types.INTEGER,
                    'DepDel15': sqla.types.INTEGER,
                    'DepTimeBlk': sqla.types.CHAR(9),
                    'TaxiOut': sqla.types.INTEGER,
                    'WheelsOff': sqla.types.TIME,
                    'WheelsOn': sqla.types.TIME,
                    'TaxiIn': sqla.types.INTEGER,
                    'CRSArrTime': sqla.types.TIME,
                    'ArrTime': sqla.types.TIME,
                    'ArrDelay': sqla.types.INTEGER,
                    'ArrDelayMinutes': sqla.types.INTEGER,
                    'ArrDel15': sqla.types.INTEGER,
                    'ArrTimeBlk': sqla.types.CHAR(9),
                    'Cancelled': sqla.types.INTEGER,
                    'CancellationCode': sqla.types.CHAR(3),
                    'Diverted': sqla.types.INTEGER,
                    'CRSElapsedTime': sqla.types.INTEGER,
                    'ActualElapsedTime': sqla.types.INTEGER,
                    'Flights': sqla.types.INTEGER,
                    'Distance': sqla.types.INTEGER,
                    'CarrierDelay': sqla.types.INTEGER,
                    'WeatherDelay': sqla.types.INTEGER,
                    'NASDelay': sqla.types.INTEGER,
                    'SecurityDelay': sqla.types.INTEGER,
                    'LateAircraftDelay': sqla.types.INTEGER}

def int_to_time(tmp, x):
    tmp[x] = pd.to_numeric(tmp[x], errors = 'coerce')
    tmp[x] = tmp[x].astype('int64')
    tmp[x] = tmp[x].apply(str)
    tmp[x] = tmp[x].str.replace('.', '')
    tmp['tmpLength'] = tmp[x].str.len()
    tmp[x] = np.where(tmp['tmpLength'] >= 4, tmp[x].str[:4], tmp[x])
    tmp['tmpLength'] = tmp[x].str.len()
    tmp['hr'] = np.where(tmp['tmpLength'] == 4, tmp[x].str[:2], 
                        np.where(tmp['tmpLength'] == 3, '0' + tmp[x].str[:1], 
                        np.where(tmp['tmpLength'] == 2, '00',
                        np.where(tmp['tmpLength'] == 1, '00', '00'))))
    tmp['hr'] = np.where(tmp['hr'] == '24', '00', tmp['hr'])
    tmp['tmpLength'] = tmp[x].str.len()
    tmp['mi'] = np.where(tmp['tmpLength'] == 4, tmp[x].str[2:4], 
                        np.where(tmp['tmpLength'] == 3, tmp[x].str[1:3],
                            np.where(tmp['tmpLength'] == 2, tmp[x], 
                                np.where(tmp['tmpLength'] == 1, '0' + tmp[x], '00'))))
    tmp['txtTime'] = tmp['hr'] + ':' + tmp['mi'] + ':00'
    tmp[x] = pd.to_datetime(tmp['txtTime'], format = '%H:%M:%S').dt.time
    

def read_metrics_csv(path):
    files = os.listdir(path)
    for file in files:
        if file.endswith('.csv'):
            file_path = path + '/' + file
            tmp = pd.read_csv(file_path, index_col = None, header = 0, low_memory = False)
            tmp = tmp.loc[tmp['Reporting_Airline'].isin(('AA', 'UA', 'DL', 'CO', 'NW', 'US'))]
            tmp = tmp.fillna(0)
            int_to_time(tmp,'CRSDepTime')
            int_to_time(tmp,'DepTime')
            int_to_time(tmp, 'WheelsOff')
            int_to_time(tmp, 'WheelsOn')
            int_to_time(tmp,'CRSArrTime')
            int_to_time(tmp,'ArrTime')
            tmp = tmp[['Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'IATA_CODE_Reporting_Airline',
                        'Tail_Number', 'Flight_Number_Reporting_Airline', 'Origin', 'Dest', 'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes',
                        'DepDel15', 'DepTimeBlk', 'TaxiOut', 'WheelsOff', 'WheelsOn', 'TaxiIn', 'CRSArrTime',
                        'ArrTime', 'ArrDelay', 'ArrDelayMinutes', 'ArrDel15', 'ArrTimeBlk', 'Cancelled', 'CancellationCode',
                        'Diverted', 'CRSElapsedTime', 'ActualElapsedTime', 'Flights', 'Distance', 'CarrierDelay', 
                        'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']]
            tmp.to_sql(name = 'flight_data', schema = 'analysis', con = psql_engine, if_exists = 'append', dtype = flight_data_dict, index = False)
            # return tmp

airline_data_df = read_metrics_csv(path)