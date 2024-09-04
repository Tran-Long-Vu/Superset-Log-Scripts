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












