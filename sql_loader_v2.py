'''   
input: 2 folders of csv files

extract csv to df

df to sql upload 
postgre conn
psycopg

output: database update (sql)
'''

import psycopg2
from psycopg2 import sql
import tqdm as tqdm
import pandas as pd
import os

DB_HOST = '127.0.0.1'
DB_NAME = 'datalake'
DB_USER = 'superset'
DB_PASSWORD = 'superset'

PATH_TO_EVENT_CSV = './output_csv/event_csv/'
PATH_TO_ALARM_CSV = './output_csv/alarm_csv/'

class SqlLoader():
    pass
    def __init__(self):
        self.connection , self.cursor =  self.connect_database()
        pass
    
    def upload_single_csv(self, csv_file):
        '''  
        input: single csv file
        output: uploaded df of csv to postGRE
        '''
        df = pd.read_csv(csv_file) 
        if 'event_data' in csv_file:
            df = self.postprocess_events(df)
            self.push_event_data_to_db(df, 'home_face_camera_event_log_data')#
        if 'alarm_data' in csv_file: 
            df = self.postprocess_alarms(df)
            self.push_event_data_to_db(df, 'home_face_camera_alarm_data')
        pass
    
    
    def upload_all_folders(self,event_csv_folder, alarm_csv_folder):
        print('     ...start upload....')
        for filename in tqdm.tqdm(os.listdir(event_csv_folder), desc ="Reading event csv files: "):
            if filename.endswith('.csv'):
                file_path = os.path.join(event_csv_folder , filename)
                self.upload_single_csv(file_path)
                
        for filename in tqdm.tqdm(os.listdir(alarm_csv_folder), desc ="Reading alarm csv files: "):
            if filename.endswith('.csv'):  
                file_path = os.path.join(alarm_csv_folder , filename)
                self.upload_single_csv(file_path)        
        print('------Finished uploading all data.---------')
        pass
    
    def connect_database(self):      
        connection = psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
        cursor = connection.cursor()
        return connection, cursor
        
    def push_event_data_to_db(self, df, table_name):
        connection = self.connection
        cursor = connection.cursor()
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ', '.join(list(df.columns))
        placeholders = ', '.join(['%s'] * len(df.columns)) 
        
        SQL = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});" 

        for row in tuples:
            cursor.execute(SQL, row)  

        connection.commit()
    def load_alarm_table(self, df):
        
        
        pass
    def disconnect_database(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        pass
    def postprocess_events(self, df):
        df = df.rename(columns={
        'sn': 'sn',
        'user_id': 'user_id',
        'time_query_latest': 'time_query_latest',
        'datetime': 'date_time',
        'TimeQueryEvent': 'time_query_event',
        'TimeHomeFace': 'time_home_face',
        'AllTimeWorker': 'all_time_worker'
        })
        
        df = df.drop('sn.1', axis=1)
        return df

    def postprocess_alarms(self, df):
        df = df.rename(columns={
        'SerialNumber': 'serial_number',
        'AlarmEvent': 'alarm_event',
        'AlarmId': 'alarm_id',
        'AlarmTime': 'alarm_time',
        'Channel': 'channel',
        'PicInfo.ObjName': 'pic_info_obj_name',
        'PicInfo.ObjSize': 'pic_info_obj_size',
        'PicInfo.StorageBucket': 'pic_info_obj_bucket',
        'VideoInfo.VideoLength': 'video_info_video_length',
        'PicErr': 'pic_err',
        'DelayAlarm': 'delay_alarm'
                    })
        if 'video_info_video_length' in df.columns:
            df = df.drop('video_info_video_length', axis = 1)
        header_values = ['sn', 'user_id', 'time_query_latest', 'alarm_event', 'alarm_id', 'alarm_time', 'channel', 'pic_info_obj_name', 'pic_info_obj_size', 'pic_info_obj_bucket', 'pic_err', 'delay_alarm']

        return df
    
    

if __name__ == '__main__':   
    
    
    loader = SqlLoader()
    loader.upload_all_folders(PATH_TO_EVENT_CSV, PATH_TO_ALARM_CSV)
    pass





























