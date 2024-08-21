'''   
reference log: 

[['1723222811806719809', 
"2024-08-10 00:00:11.806 | INFO     | 
actor.fetch_event:perform:28 - 
arg: {'sn': 'fb1f6e5c62b71703', 
    'user_id': '65420522b8a6631f38d5e164', 
    'token': 'ZTMzOTRmOWRkY3xmYjFmNmU1YzYyYjc
    xNzAzfDY1NDIwNTIyYjhhNjYz6MWYzOGQ1ZTE2NHw
    xNzIzMzA4NDAzMTE0fDF8MTcyLjE2LjMwLjV8MQ%3
    D%3D.c0c3ca7c8d65eb48bb914c2695e23205', 
    
    'time_millis': '00002511723222002209', 
    'encrypted_str': 'fc323c6a7d883416cb9dcadb401d0de9',
    'time_query_latest': '2024-08-09 23:59:52', 
    'datetime': '2024-08-10 00:00:01'}"], 


['1723222811807524154', "2024-08-10 00:00:11.807 
| INFO     | actor.fetch_event:query_event:58 - 
Request get event 
list: {'sn': 'fb1f6e5c62b71703',
    'startTime': '2024-08-09 23:59:52', 
    'endTime': '2024-08-10 00:00:01'}"], 
    
['1723222811807760490', '2024-08-10 00:00:11.807 
| INFO     | actor.fetch_event:query_event:59 - 
URL: https://rds.viettelcamera.vn/v2/rtc/device/getDeviceAlarmList/ZTMzOTRmOWRkY3xmYjFmNmU1YzYyYjcxNzAzfDY1NDIwNTIyYjhhNjYz6MWYzOGQ1ZTE2NHwxNzIzMzA4NDAzMTE0fDF8MTcyLjE2LjMwLjV8MQ%3D%3D.c0c3ca7c8d65eb48bb914c2695e23205'], 


['1723222811814877680', '[2024-08-10 00:00:11,814: 
INFO/MainProcess] 

Task fetch_event_home_face
[c67b227034e048bc8a2ff10cb3dace2b] received'], 

['1723222811852070802', '2024-08-10 00:00:11.851 
| INFO     | 
    
    actor.fetch_event:query_event:78 - 
    Time query event: 0.04346513748168945'], 

['1723222811856000312', "2024-08-10 00:00:11.854 
| INFO     | actor.fetch_event:query_event:80 - 

Response get event list:
{'code': 2000, 'msg': 'Success', 
    'data': {'SerialNumber': 'fb1f6e5c62b71703', 
    'AlarmTotal': 0, 'AlarmArray': [],
    'IsFinished': '1'}}"], 
['1723222811856529565', '2024-08-10 00:00:11.856 | INFO 
| actor.fetch_event:perform:31 - 
    All time worker: 0.049759864807128906 s']]

search & sort by: 
- regex
- arg:
- {}
- INFO |

"2024-08-10 18:08:33.154 | INFO   
| actor.fetch_event:send_request_event_manager:120 -
Request: {
    'alarm_event': 'VideoMotion:2', '
    alarm_id': '2481018817',
    'alarm_msg': '', 
    'alarm_time': '2024-08-10 18:08:18', 
    'auth_code': 'test', 
    'channel': '0', 
    'serial_number': 'ba39b3962a4bfe81', 
    'status': 'test'}"


    # args
    timestamp = ''
    sn = ''
    user_id = ''
    token = ''
    time_millis = ''
    encrypted_str = ''
    time_query_latest = ''
    datetime = ''
    
    # request get event list
    request_sn = ''
    request_start_time = ''
    request_end_time = ''
    
    # response get event list
    response_code = ''
    response_msg = ''
    
    # data
    response_sn = ''
    alarm_total = ''
    alarm_array = []
    is_finished = ''
    
    
    # alarm
    alarm_event = ''
    alarm_id = ''
    alarm_msg = ''
    alarm_time = ''
    alarm_auth_code = ''
    alarm_channel = ''
    serial_number = ''
    status = ''


in case of alarm , response:
"2024-08-10 18:53:33.287 | INFO     | actor.fetch_event:query_event:80 - 
Response get event list: {'
    code': 2000, 'msg': 'Success', 
        'data': {'SerialNumber': 'ba39b3962a4bfe81',
                'AlarmTotal': 1,
                'AlarmArray':
                
                [ { 'AlarmEvent': 'VideoMotion:2', 
                'AlarmId': '24810185313', 
                'AlarmMsg': '',
                'AlarmTime': '2024-08-10 18:53:14',
                'Channel': '0', 
                'PicInfo': {'ObjName': '003/003_ba39b3962a4bfe81_24810185313.jpeg', 
                'ObjSize': 47055, 
                'StorageBucket': 'OBS_sg-ai-img'} } ], 
                
                
                'IsFinished': '1'}}"



'''