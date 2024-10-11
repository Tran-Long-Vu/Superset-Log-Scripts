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
import numpy as np
DB_HOST = '127.0.0.1'
DB_NAME = 'datalake' # change db
DB_USER = 'superset'
DB_PASSWORD = 'superset'

PATH_TO_EVENT_CSV = './output_csv/event_csv/'
# PATH_TO_ALARM_CSV = './output_csv/alarm_csv/'

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
            self.push_event_data_to_db(df, 'new_face_recognition_event_data')#
        # if 'alarm_data' in csv_file: 
        #     df = self.postprocess_alarms(df)
        #     self.push_event_data_to_db(df, 'home_face_camera_alarm_data')
        pass
    
    
    def upload_all_folders(self,event_csv_folder):
        print('     ...start upload....')
        for filename in tqdm.tqdm(os.listdir(event_csv_folder), desc ="Reading event csv files: "):
            if filename.endswith('.csv'):
                file_path = os.path.join(event_csv_folder , filename)
                self.upload_single_csv(file_path)
                
        # for filename in tqdm.tqdm(os.listdir(alarm_csv_folder), desc ="Reading alarm csv files: "):
        #     if filename.endswith('.csv'):  
        #         file_path = os.path.join(alarm_csv_folder , filename)
        #         self.upload_single_csv(file_path)        
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
            # print('row is' + str(row))
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
        ''' TODO: postprocess by fitlering columns. 
        - either drop columns or take some. 
        - change column names to lowercase & _
        - drop cloned columns.* 
        - do not drop NA.
        '''
        columns_to_keep = [
        'PicName', 'Channel', 'AlarmTime', 'AlarmID', 'CssPicSize',
        'AlarmMsg', 'DevName', 'AlarmEvent', 'SerialNumber', 'AlarmVendor',
        'PicSize', 'Status', 'Level', 'AisPush', 'si_id', 'group',
        'user_id', 'source_id', 'object_id', 'bbox', 'lm',
        'confidence', 'image_path', 'timestamp', 'silent_face',
        'person_id', 'message_base64.service_key',
        'message_base64.cam_info.cam_id', 
        'message_base64.cam_info.location',
        'message_base64.ai_config.attribute'
        ]
        existing_columns = df.columns.tolist()
        missing_columns = [col for col in columns_to_keep if col not in existing_columns]
        for col in missing_columns:
            df[col] = np.nan 
        df.fillna(value=np.nan, inplace=True)  
        if len(df.columns) > 30:
            df = df[columns_to_keep] 
        # print("Total number of columns:", len(df.columns))
        df.columns = df.columns.str.lower()
        df = df.drop(columns=['group'])
        df = df.drop(columns=['timestamp'])
        df = df.drop(columns=['aispush'])
        df = df.drop(columns=['alarmid'])
        dummy_timestamp = '2024-01-01 00:00:00'
        # Replace np.nan values in the 'alarmtime' column with the dummy timestamp
        df['alarmtime'] = df['alarmtime'].fillna(dummy_timestamp)
        # Renaming the columns
        df.rename(columns={
            'message_base64.service_key': 'message_base64_service_key',
            'message_base64.cam_info.cam_id': 'message_base64_cam_info_cam_id',
            'message_base64.cam_info.location': 'message_base64_cam_info_location',
            'message_base64.ai_config.attribute': 'message_base64_ai_config_attribute'
        }, inplace=True)
        return df

    
    

if __name__ == '__main__':   
    
    
    loader = SqlLoader()
    loader.upload_all_folders(PATH_TO_EVENT_CSV)
    pass































