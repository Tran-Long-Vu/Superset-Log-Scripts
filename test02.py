import re

def extract_log_content(text):
    # Define the regex pattern to find the starting point
    start_pattern = r'event:jf_event_receiver:32 - request:\s*'
    
    # Search for the starting pattern in the text
    start_match = re.search(start_pattern, text)
    
    if start_match:
        # Find the starting index right after "request: "
        start_index = start_match.end()
        
        # Initialize brace counter and set end_index to start_index
        brace_count = 0
        end_index = start_index
        
        while end_index < len(text):
            if text[end_index] == '{':
                brace_count += 1
            elif text[end_index] == '}':
                brace_count -= 1
            
            # If we have closed all opened braces, we found our end
            if brace_count == 0 and brace_count != -1:
                break
            
            end_index += 1
        
        # Extract and return content from start to end index
        return text[start_match.start():end_index + 1]
    
    return None

# Example input string
input_string = """
1727545176881398495",
                        "2024-09-29 00:39:36.881 | INFO     | event_manager.routers.v1.jf.event:jf_event_receiver:36 - Image error!!!"
                    ],
                    [
                        "1727545176881015281",
                        "2024-09-29 00:39:36.880 | INFO     | event_manager.routers.v1.jf.event:jf_event_receiver:32 - request: {'PicName': '', 'CssBucketInfo': 'Default', 'AlarmTime': '2024-09-29 00:39:36', 'AlarmMsg': '{\"MsgType\":\"Offline\"}', 'AlarmEvent': 'DevStatusAlarm:2', 'SerialNumber': 'fc5f9a3b7707d489', 'CssPicSize': 0, 'AlarmID': '240929003936', 'Channel': 0}"
                    ],
                    [
                        "1727545176880548319",
                        "2024-09-29 00:39:36.880 | INFO     | event_manager.routers.v1.jf.event:jf_event_receiver:26 - Request Body Param: {\"PicName\":\"\",\"CssBucketInfo\":\"Default\",\"AlarmTime\":\"2024-09-29 00:39:36\",\"AlarmMsg\":\"{\\\"MsgType\\\":\\\"Offline\\\"}\",\"AlarmEvent\":\"DevStatusAlarm:2\",\"SerialNumber\":\"fc5f9a3b7707d489\",\"CssPicSize\":0,\"AlarmID\":\"240929003936\",\"Channel\":0}"
                    ],
                    [
"""

# Extracting request content
request_content = extract_log_content(input_string)

# Display extracted request content
if request_content:
    print("Extracted Request Content:")
    print(request_content)
else:
    print("No request content found.")
