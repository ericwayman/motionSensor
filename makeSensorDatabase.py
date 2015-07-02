#Script to create sql database: sensorData.db.
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine


disk_engine = create_engine('sqlite:///sensorData.db')

start = dt.datetime.now()
#files are stored in a dir "DataFiles"
#they are named i.csv for i from 1 to 15
index_start = 0
for x in range(1,16):
    filename = str(x)+".csv"
    columnNames = ['sequential_number','x_acceleration','y_acceleration','z_acceleration','label']
    df = pd.read_csv('DataFiles/'+filename,header = None,names = columnNames) 
    #need to update the sequential_number field since high numbers aren't held to high enough precision in the csv
    df['sequential_number'] = range(len(df.index))
    df.to_sql('Participant_'+str(x), disk_engine, if_exists='replace')
    #increment indices for full_data
    df.index += index_start
    index_start += len(df.index)
    
    #replace old database if starting from scratch
    if x == 1:
        rule = 'replace'
    #add other participants to database we've created
    else:
        rule = 'append'
    df['participant_id']=x
    df.to_sql('full_data', disk_engine, if_exists=rule)
    print '{} seconds: completed {} rows for {}'.format((dt.datetime.now() - start).seconds, len(df.index),filename)    
