import psycopg2
DB_HOST = '127.0.0.1'
DB_NAME = 'datalake'
DB_USER = 'superset'
DB_PASSWORD = 'superset'
conn = psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

cursor = conn.cursor()

query = "DELETE FROM home_face_camera_alarm_data; DELETE FROM home_face_camera_event_log_data;"
cursor.execute(query)

conn.commit()

cursor.close()
conn.close()
