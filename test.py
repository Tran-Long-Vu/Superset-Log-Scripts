'''
id

sn

user_id

token

time_millis

encrypted_str

time_query_latest

date_time

start_time

time_query_event

time_home_face

all_time_worker


### alarm 
    
id

serial_number

alarm_event

alarm_id

alarm_time

channel

pic_info_obj_size

pic_info_obj_name

pic_info_obj_bucket

pic_err

video_info_video_length

delay_alarm












,SerialNumber,
AlarmEvent,
AlarmId,
AlarmTime,
Channel,
PicInfo.ObjName,
PicInfo.ObjSize,
PicInfo.StorageBucket,
VideoInfo.VideoLength,
PicErr,
DelayAlarm



ALTER TABLE home_face_camera_alarm_data
ADD COLUMN serial_number VARCHAR;





'''

DB_HOST = '127.0.0.1'
DB_NAME = 'datalake'
DB_USER = 'superset'
DB_PASSWORD = 'superset'

import psycopg2
from psycopg2 import OperationalError

def test_postgresql_connection(host, database, user, password):

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
    host=DB_HOST, 
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    )
    print("Connected to PostgreSQL database successfully.")
    # Create a cursor object
    cursor = conn.cursor()

    # Execute the TRUNCATE statement
    cursor.execute("TRUNCATE TABLE home_face_camera_alarm_data;")

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Replace with your PostgreSQL database credentials
test_postgresql_connection('localhost', 'your_database', 'your_username', 'your_password')











