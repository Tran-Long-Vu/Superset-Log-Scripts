
import re 

log_entry= "2024-09-28 15:26:44.329 | INFO     | event_manager.services.v1.vss:process_ai_event:23 - metadata: {'img_uuid': 'face/1727512004_73482902-f612-4337-8225-4804d43cb3f9_vht_cam_01_0.jpg', 'event_type': 'FACE_RECOGNITION', 'description': '{\"results\": [{\"si_id\": \"vht.mdo.com.vn\", \"group\": \"vht.mdo.com.vn\", \"access_key\": \"access123\", \"user_id\": \"3126\", \"source_id\": \"vht_cam_01\", \"video_id\": \"j9xH7VoDwXAYt6sFDfmQxMLLTHTaM9k8cm\", \"object_id\": \"29\", \"bbox\": [846, 354, 900, 423], \"lm\": [865.0, 380.0, 890.0, 387.0, 874.0, 396.0, 861.0, 404.0, 880.0, 410.0], \"confidence\": 0.7625893950462341, \"image_path\": \"3126_vht_cam_01_417e5373d4c14c54909eb54ceef62fbe.jpg\", \"timestamp\": 1727512004.110692, \"message_base64\": {\"si\": {\"si_id\": \"vht.mdo.com.vn\", \"si_name\": \"vht.mdo.com.vn\", \"si_token\": \"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJCU1NEIiwiZXhwIjoxNzI5NTQyODUyfQ.Qe_ajvVuv7JV26BxzcVcz_L6Y3bdodFbiH_mezIzEqk\", \"si_api\": {\"endpoint\": \"https://service-mass.mdo.com.vn/api/timekeeping/camera/limitless/public/event-user-time-keeping\", \"method\": \"POST\"}}, \"user\": {\"user_id\": \"3126\", \"user_name\": \"vht\", \"user_group\": \"vht.mdo.com.vn\"}, \"service_key\": \"FACE_RECOGNITION\", \"cam_info\": {\"cam_id\": \"vht_cam_01\", \"location\": \"HN\", \"source\": \"rtsp://teststream:Camera@1234@27.72.146.175:8555/fhd\", \"accessKey\": \"access123\", \"endpoint\": \"\", \"ai_streaming_out\": true, \"video_event\": true}, \"ai_config\": {\"dist_type\": \"string\", \"attribute\": [\"gender\", \"age\"], \"landmark\": true, \"limitation\": 100, \"min_size\": 20, \"max_size\": 500, \"nms_threshold\": 0.5, \"conf_threshold\": 0.5}, \"hi_config\": {\"zones\": [], \"lines\": [], \"time_interval\": 30.0, \"overlap_threshold\": 0.5, \"margin\": 10.0, \"time_threshold\": 60.0}}, \"silent_face\": \"living\", \"person_id\": [\"3129\", \"3129\", \"3129\", \"3129\", \"3129\"], \"person_name\": [\"Ph\\\\u1ea1m Th\\\\u1ecb Th\\\\u00f9y\", \"Ph\\\\u1ea1m Th\\\\u1ecb Th\\\\u00f9y\", \"Ph\\\\u1ea1m Th\\\\u1ecb Th\\\\u00f9y\", \"Ph\\\\u1ea1m Th\\\\u1ecb Th\\\\u00f9y\", \"Ph\\\\u1ea1m Th\\\\u1ecb Th\\\\u00f9y\"], \"dist\": [1.741870641708374, 1.7847011089324951, 1.79740309715271, 1.8095592260360718, 1.815774917602539]}]}'}" 
 


# Regex pattern to capture timestamp and metadata
pattern = r'event:jf_event_receiver:32 - request:\s*'
# Search for the pattern
match = re.search(pattern, log_entry)

if match:
    # Get the matched timestamp
    timestamp = match.group(1)
    
    # Extract metadata from the log entry
    metadata_start = log_entry.find('(')
    metadata_end = log_entry.find(')', metadata_start) + 1  # Include closing parenthesis
    metadata = log_entry[metadata_start:metadata_end]
    
    print("Timestamp found:", timestamp)
    print("Metadata found:", metadata)
else:
    print("Pattern not found.")