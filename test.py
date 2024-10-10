'''
code chuong lon để test extract
'''

import os, json
import pandas as pd
import numpy as np
import glob
import re
import tqdm as tqdm
import ast
import gc
import resource

FILE = "json_logs/data_2024-09-28.json"


# from sql_loader import SqlLoader
def limit_memory(maxsize):
    # Set the maximum memory limit (in bytes)
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))

limit_memory(12 * 1024 * 1024 * 1024)  # Limit to 12 GB 




def read_single_file(json_file): # call at bottom
    # read json (json_path)
    # Load JSON data from a file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Initialize a list to hold values corresponding to stderr
    stderr_values = []

    # Iterate through the result array
    for entry in data['data']['result']:
        # Check if the stream type is 'stderr'
        if entry['stream']['stream'] == 'stderr':
            # Extend the list with values from the 'values' list
            stderr_values.extend(entry['values'])

    # Convert the collected stderr values into a DataFrame
    stderr_df = pd.DataFrame(stderr_values, columns=['timestamp_id', 'log_message'])


    ordered_logs = stderr_df[::-1]
    refined_logs = ordered_logs["log_message"]
    return refined_logs

refined_logs = read_single_file(FILE)
# collect all events

def extract_events(refined_logs):
    # Initialize an empty DataFrame to collect events
    events_df = pd.DataFrame(columns=['event_id', 'log_entry'])
    current_event = []
    event_id = 0  # To keep track of event IDs

    # Iterate through each log entry in the Series
    for index, log_entry in refined_logs.items():
        # Check if the current log entry is the start of an event
        if 'Received metadata from jf' in log_entry:
            # If there's an ongoing event, save it before starting a new one
            if current_event:
                # Combine all log lines for the current event into a single string
                combined_log_entry = "\n".join(current_event)
                # Create a temporary DataFrame for the current event
                temp_df = pd.DataFrame({'event_id': [event_id], 'log_entry': [combined_log_entry]})
                events_df = pd.concat([events_df, temp_df], ignore_index=True)
                current_event = []  # Reset for the new event
                event_id += 1  # Increment event ID
            
        # Append the current log entry to the ongoing event
        current_event.append(log_entry)

    # Don't forget to add the last event if it exists
    if current_event:
        combined_log_entry = "\n".join(current_event)
        temp_df = pd.DataFrame({'event_id': [event_id], 'log_entry': [combined_log_entry]})
        events_df = pd.concat([events_df, temp_df], ignore_index=True)

    return events_df

# Example usage
# Assuming refined_logs is your Series containing log messages
# refined_logs = pd.Series([...])  # Your log messages here


events_df = extract_events(refined_logs)
# print(events_df["log_entry"].iloc[0])


def get_alarm_args(log_entry):
    '''  
    Input: log entry string
    Output: arg data fields as DataFrame for a single data point
    '''
    # Define the regex pattern to extract request content
    request_pattern = r'event:jf_event_receiver:32 - request:\s*'
    
    # Search for the pattern in the log entry
    match = re.search(request_pattern, log_entry)
    
    if match:
        # Find the starting index right after "request: "
        start_index = match.end()
        
        # Initialize brace counter and set end_index to start_index
        brace_count = 0
        end_index = start_index
        
        while end_index < len(log_entry):
            if log_entry[end_index] == '{':
                brace_count += 1
            elif log_entry[end_index] == '}':
                brace_count -= 1
            
            # If we have closed all opened braces, we found our end
            if brace_count == 0:
                break
            
            end_index += 1
        
        # Extract the content from start to end index
        args_string = log_entry[start_index:end_index + 1]
        
        # Print the extracted string for debugging
        # print("Extracted Args String:", args_string)
        #args_string = args_string.replace('"', '\\"').replace('\\', '\\\\')
        # Print the final escaped string
        # print(args_string)

        args_dict = eval(args_string)  # Use eval to convert string to dictionary safely
        df = pd.json_normalize(args_dict)  # Normalize the dictionary into a DataFrame
        return df
    
    return pd.DataFrame()  # Return an empty DataFrame if no match is found

def fetch_alarm_args(events_df):
    '''  
    Input: DataFrame containing events with log entries
    Output: Combined DataFrame of all extracted args from each event
    '''
    args_dfs = []  # List to hold DataFrames from each log entry
    
    # Iterate through each log entry in events_df
    for index, row in events_df.iterrows():
        log_entry = row['log_entry']
        args_df = get_alarm_args(log_entry)  # Get args DataFrame from the log entry
        
        if not args_df.empty:
            args_dfs.append(args_df)  # Append only non-empty DataFrames
    
    # Concatenate all collected DataFrames into one
    final_args_df = pd.concat(args_dfs, ignore_index=True) if args_dfs else pd.DataFrame()
    
    return final_args_df

def get_face_args( log_entry):
        '''  
        Input: log entry string
        Output: arg data fields as DataFrame for a single data point
        '''
        
        request_pattern = r'\'description\': \'{\"results\": \s*'
        match = re.search(request_pattern, log_entry) 
        if match:
            start_index = match.end()
            brace_count = 0
            end_index = start_index
            
            while end_index < len(log_entry):
                if log_entry[end_index] == '{':
                    brace_count += 1
                elif log_entry[end_index] == '}':
                    brace_count -= 1
                if brace_count == 0:
                    break
                end_index += 1
            while end_index < len(log_entry):
                if log_entry[end_index] == '[':
                    brace_count += 1
                elif log_entry[end_index] == ']':
                    brace_count -= 1
                if brace_count == 0:
                    break
                end_index += 1
                
            args_string = log_entry[start_index:end_index + 1]
            args_string = args_string.replace('true', 'True').replace('null', 'None').replace('false', 'False')
            
            args_dict = eval(args_string) 
            
            df = pd.json_normalize(args_dict)
            
            return df
        return pd.DataFrame() 

def fetch_face_args( events_df):
    '''  
    Input: DataFrame containing events with log entries
    Output: Combined DataFrame of all extracted args from each event
    '''
    args_dfs = []  # List to hold DataFrames from each log entry
    
    # Iterate through each log entry in events_df
    for index, row in tqdm.tqdm(events_df.iterrows(), total=events_df.shape[0], desc="Processing Rows"):      
        log_entry = row['log_entry']
        args_df = get_face_args(log_entry)  # Get args DataFrame from the log entry
        
        if args_df is not None and not args_df.empty:
            args_dfs.append(args_df)  # Append only non-empty DataFrames
    
    # Concatenate all collected DataFrames into one
    final_args_df = pd.concat(args_dfs, ignore_index=True) if args_dfs else pd.DataFrame()
    
    return final_args_df

arg_df = fetch_face_args(events_df)
print(str(len(arg_df.columns)))
# set new default values to record in colums.


