'''
Log Script for October logs.
'''
import os
import pandas as pd
import json
import numpy as np
import glob
import re
import tqdm as tqdm
import ast
from pathlib import Path
import resource

PATH_TO_EVENT_CSV = 'output_csv/event_csv/'
PATH_TO_ALARM_CSV = 'output_csv/alarm_csv/'


def limit_memory(maxsize):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))


class Extractor():
    def __init__(self) -> None:
        self.path_to_json = './json_logs'
        pass
    
    
    def read_single_file(self, json_file): 
        with open(json_file, 'r') as file:
            data = json.load(file)
        stderr_values = []
        for entry in data['data']['result']:
            if entry['stream']['stream'] == 'stderr':
                stderr_values.extend(entry['values'])
        stderr_df = pd.DataFrame(stderr_values, columns=['timestamp_id', 'log_message'])
        ordered_logs = stderr_df[::-1]
        refined_logs = ordered_logs["log_message"]
        return refined_logs
    
    def write_all(self, json_folder_path):
        index = 0
        for filename in tqdm.tqdm(os.listdir(json_folder_path), desc ="Reading json files: "):
            if filename.endswith('.json'):  
                file_path = os.path.join(json_folder_path, filename)
                json_df = self.read_single_file(file_path)
                event_all_df = self.collect_all_events(json_df)
                # print(event_all_df)
                event_df  = self.fetch_all(event_all_df)
                #alarm_df.drop('AlarmMsg', axis = 1, inplace = True)
                self.write_to_csv_folder(index,
                event_df,
                #alarm_df, 
                PATH_TO_EVENT_CSV,
                #PATH_TO_ALARM_CSV,
                )
            index += 1
        pass
    

    def collect_all_events(self, series):
        '''  
        Input: series variable of all read logs.
        Output: a dataframe with events as data points in string format.
        '''
        events_df = pd.DataFrame(columns=['event_id', 'log_entry'])
        current_event = []
        event_id = 0  
        for index, log_entry in series.items():
            if 'Received metadata from jf' in log_entry:
                if current_event:
                    combined_log_entry = "\n".join(current_event)
                    temp_df = pd.DataFrame({'event_id': [event_id], 'log_entry': [combined_log_entry]})
                    events_df = pd.concat([events_df, temp_df], ignore_index=True)
                    current_event = []  
            current_event.append(log_entry)
        if current_event:
            combined_log_entry = "\n".join(current_event)
            temp_df = pd.DataFrame({'event_id': [event_id], 'log_entry': [combined_log_entry]})
            events_df = pd.concat([events_df, temp_df], ignore_index=True)
        return events_df

 
    def write_to_csv_folder(self, 
                        file_index,
                        event_df,
                        #alarm_df,
                        event_csv_path,
                        #alarm_csv_path
                        ):  
        """
        Write two DataFrames to specified CSV file paths in chunks.
        Parameters:
        - file_index (int): The index used to create the CSV file names.
        - event_df (pd.DataFrame): DataFrame containing event data.
        - alarm_df (pd.DataFrame): DataFrame containing alarm data.
        - event_csv_path (str): Directory path to save the event CSV file.
        - alarm_csv_path (str): Directory path to save the alarm CSV file.
        - chunk_size (int): Number of rows to write at a time.
        """
        event_file_name = f"event_data_{file_index}.csv"
        #alarm_file_name = f"alarm_data_{file_index}.csv"
        
        event_file_path = os.path.join(event_csv_path, event_file_name)
        # alarm_file_path = os.path.join(alarm_csv_path, alarm_file_name)
        
            
        event_df.to_csv(
            event_file_path, 
            mode='w', 
            header=True, 
            index=False  
        )

        # alarm_df.to_csv(
        #     alarm_file_path, 
        #     mode='w', 
        #     header=True,
        #     index=False  
        # )

    
        del event_df
        # del alarm_df
    
    def fetch_all(self, event_all_df):
        ''' 
        '''
        
        df_result = pd.concat(
                        [
                        self.fetch_alarm_args(event_all_df),
                        self.fetch_face_args(event_all_df)
                        # self.fetch_args(event_all_df),
                        # self.fetch_request_event_list(event_all_df),
                        # self.fetch_time_query_event(event_all_df),
                        # self.fetch_time_process_home_face(event_all_df),
                        # self.fetch_all_time_worker(event_all_df)
                        ],
                        axis=1)
        
        # response_alarm = self.fetch_response_alarm(event_all_df)
        # delay_alarm = self.fetch_delay_alarm(event_all_df)       
        # n = len(response_alarm)
        # response_alarm_subset = response_alarm.head(n)  
        # delay_alarm_subset = delay_alarm.head(n)    
        
        ''' post processing'''
        # alarm_df = pd.concat(
        #     [response_alarm_subset, delay_alarm_subset],
        #     axis=1
        # )
        
        # df_result = df_result.drop('token',axis = 1)
        # df_result = df_result.drop('time_millis',axis = 1)
        # df_result = df_result.drop('encrypted_str',axis = 1)
        # df_result = df_result.drop('startTime',axis = 1)
        # df_result = df_result.drop('endTime',axis = 1)
        # df_result = df_result.replace('Not Found', pd.NA).dropna() 

        return df_result # , alarm_df
    
    def get_alarm_args(self, log_entry):
        '''  
        Input: log entry string
        Output: arg data fields as DataFrame for a single data point
        '''
        
        request_pattern = r'event:jf_event_receiver:32 - request:\s*'
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
            args_string = log_entry[start_index:end_index + 1]
            args_dict = eval(args_string) 
            df = pd.json_normalize(args_dict) 
            return df
        
        return pd.DataFrame()  # Return an empty DataFrame if no match is found

    def fetch_alarm_args(self, events_df):
        '''  
        Input: DataFrame containing events with log entries
        Output: Combined DataFrame of all extracted args from each event
        '''
        args_dfs = []  # List to hold DataFrames from each log entry
        
        # Iterate through each log entry in events_df
        for index, row in events_df.iterrows():
            log_entry = row['log_entry']
            args_df = self.get_alarm_args(log_entry)  # Get args DataFrame from the log entry
            
            if not args_df.empty:
                args_dfs.append(args_df)  # Append only non-empty DataFrames
        
        # Concatenate all collected DataFrames into one
        final_args_df = pd.concat(args_dfs, ignore_index=True) if args_dfs else pd.DataFrame()
        
        return final_args_df
    
    def get_face_args(self, log_entry):
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

    def fetch_face_args(self, events_df):
        '''  
        Input: DataFrame containing events with log entries
        Output: Combined DataFrame of all extracted args from each event
        '''
        args_dfs = []  
        for index, row in events_df.iterrows():
            log_entry = row['log_entry']
            args_df = self.get_face_args(log_entry) 
            
            if args_df is not None and not args_df.empty:
                args_dfs.append(args_df)  
        final_args_df = pd.concat(args_dfs, ignore_index=True) if args_dfs else pd.DataFrame()
        return final_args_df


if __name__ == '__main__':   
    limit_memory(12 * 1024 * 1024 * 1024) 

    extractor = Extractor()
    extractor.write_all('json_logs')
    pass

