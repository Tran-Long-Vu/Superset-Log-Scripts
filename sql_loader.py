import psycopg2
from psycopg2 import sql
import tqdm as tqdm
import pandas as pd
# inherit class sql into class log script.
DB_HOST = '127.0.0.1'
DB_NAME = 'datalake'
DB_USER = 'superset'
DB_PASSWORD = 'superset'

class SqlLoader():
    pass
    def __init__(self):
        self.event_csv = pd.read_csv('./output_csv/event_log_data.csv')
        self.alarm_csv = pd.read_csv('./output_csv/alarm_data.csv')
        self.event_data = self.postprocess_events(self.event_csv)
        self.alarm_df = self.postprocess_alarms(self.alarm_csv)
        self.connection , self.cursor =  self.connect_database()
        
        
        pass
    def connect_database(self):
        connection = psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
        # Create a cursor object
        cursor = connection.cursor()
        return connection, cursor
        
    def push_event_data_to_db(self, df, table_name):
        # Establish the database connection
        connection = self.connection
        # Create a cursor object
        cursor = connection.cursor()
        tuples = [tuple(x) for x in df.to_numpy()]

        # Construct the SQL query
        cols = ', '.join(list(df.columns))
        placeholders = ', '.join(['%s'] * len(df.columns))  # Create placeholders for each column
        SQL = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});"

        # Insert each row with a progress bar
        for row in tqdm.tqdm(tuples, desc="Inserting rows", unit="row"):
            cursor.execute(SQL, row)  # Pass the row as a parameter

        # Commit the transaction
        connection.commit()

        
        print(f"Data successfully inserted into {table_name}.")
    def load_alarm_table(self, df):
        
        
        pass
    def disconnect_database(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        pass
    def postprocess_events(self, df):
            # df columns to db
            df = df.rename(columns={
            'sn': 'sn',
            'user_id': 'user_id',
            'token': 'token',
            'time_millis': 'time_millis',
            'encrypted_str': 'encrypted_str',
            'time_query_latest': 'time_query_latest',
            'datetime': 'date_time',
            'startTime': 'start_time',
            'TimeQueryEvent': 'time_query_event',
            'TimeHomeFace': 'time_home_face',
            'AllTimeWorker': 'all_time_worker'
        })
            df.drop('Unnamed: 0', axis=1,inplace=True)
            df.drop('sn.1', axis=1,inplace=True)
            df.drop('endTime', axis=1,inplace=True)
            df = df.replace('Not Found', pd.NA).dropna()
            return df

    def postprocess_alarms(self, df = pd.DataFrame):
        # df columns to db
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
        df.drop('Unnamed: 0', axis=1,inplace=True)
        df = df.drop(columns=['video_info_video_length'])
        df = df.drop(columns=['pic_err'])
        df = df.replace('Not Found', pd.NA).dropna()
        
        return df



if __name__ == '__main__':   
    print('     ...start upload....')
    loader = SqlLoader()
    event_df = loader.event_data
    alarm_df = loader.alarm_df
    # print(event_df)
    # print(alarm_df)
    loader.push_event_data_to_db(event_df,'home_face_camera_event_log_data')
    loader.push_event_data_to_db(alarm_df,'home_face_camera_alarm_data')
    loader.disconnect_database()
    pass










