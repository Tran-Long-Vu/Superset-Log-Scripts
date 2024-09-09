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
        df = pd.read_json(json_file) 
        result_column_df = df['data']['result']
        all_values = []
        for i in range(len(result_column_df)): 
            values = df['data']['result'][i]['values']  
            all_values.extend(values)  
        values_df = pd.Series(all_values)
        values_df_rev = values_df.iloc[::-1]
        return values_df_rev
    
    def write_all(self, json_folder_path):
        index = 0
        for filename in tqdm.tqdm(os.listdir(json_folder_path), desc ="Reading json files: "):
            if filename.endswith('.json'):  
                file_path = os.path.join(json_folder_path, filename)
                json_df = self.read_single_file(file_path)
                event_all_df = self.collect_all_events(json_df)
                
                event_df , alarm_df = self.fetch_all(event_all_df)
                alarm_df.drop('AlarmMsg', axis = 1, inplace = True)
                
                self.write_to_csv_folder(index,
                event_df,
                alarm_df, 
                PATH_TO_EVENT_CSV,
                PATH_TO_ALARM_CSV)
            index += 1
        pass
    

    def collect_all_events(self, series):
        '''  
        Input: series variable of all read logs.
        Output: a dataframe with events as data points in string format.
        '''
        start_indices = []
        end_indices = []
        for i, value in enumerate((series)):
            if 'Start fetch event' in str(value):
                start_indices.append(i)
            elif '======END======' in str(value):
                end_indices.append(i)  
        log_segments = []
        end_index = 0 

        for start in (start_indices):
            while end_index < len(end_indices) and end_indices[end_index] <= start:
                end_index += 1
            if end_index < len(end_indices):
                end = end_indices[end_index]
                segment = ' '.join(str(series[i]) for i in range(start + 1, end))
                log_segments.append(segment)
        event_data_df = pd.DataFrame(log_segments, columns=['Log String'])
        event_data_df.index.name = 'Index'  
        event_all_df = event_data_df['Log String']
        return event_all_df  

 
    def write_to_csv_folder(self, file_index,
                        event_df,
                        alarm_df,
                        event_csv_path,
                        alarm_csv_path):  
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
        alarm_file_name = f"alarm_data_{file_index}.csv"
        
        event_file_path = os.path.join(event_csv_path, event_file_name)
        alarm_file_path = os.path.join(alarm_csv_path, alarm_file_name)
        
            
        event_df.to_csv(
            event_file_path, 
            mode='w', 
            header=True, 
            index=False  
        )

        alarm_df.to_csv(
            alarm_file_path, 
            mode='w', 
            header=True,
            index=False  
        )

    
        del event_df
        del alarm_df
    
    def fetch_all(self, event_all_df):
        ''' 
        '''
        
        df_result = pd.concat(
                        [
                        self.fetch_args(event_all_df),
                        self.fetch_request_event_list(event_all_df),
                        self.fetch_time_query_event(event_all_df),
                        self.fetch_time_process_home_face(event_all_df),
                        self.fetch_all_time_worker(event_all_df)
                        ],
                        axis=1)
        response_alarm = self.fetch_response_alarm(event_all_df)
        delay_alarm = self.fetch_delay_alarm(event_all_df)       
        n = len(response_alarm)
        response_alarm_subset = response_alarm.head(n)  
        delay_alarm_subset = delay_alarm.head(n)    
        
        alarm_df = pd.concat(
            [response_alarm_subset, delay_alarm_subset],
            axis=1
        )
        
        df_result = df_result.drop('token',axis = 1)
        df_result = df_result.drop('time_millis',axis = 1)
        df_result = df_result.drop('encrypted_str',axis = 1)
        df_result = df_result.drop('startTime',axis = 1)
        df_result = df_result.drop('endTime',axis = 1)
        df_result = df_result.replace('Not Found', pd.NA).dropna() 

        return df_result , alarm_df
    def get_args(self, log):
        '''  
        input: log strings in JSON
        output: arg data fields as dataFrame for single data point
        '''
    
        if 'arg: {' in log:
            arg_match = re.search(r'arg:\s*(\{.*?\})', log)
            if arg_match:
                args_string = arg_match.group(1)
                args_dict = ast.literal_eval(args_string) 
                df = pd.json_normalize(args_dict)
                return df
            
    def fetch_args(self, event_all_df):
        '''  
        input: 
        output: dataFrame of all event args
        '''
        args_dfs = []
        for log in ((event_all_df)):
            log = str(log)
            if isinstance(log, list):
                log = log[0]
            args_df = self.get_args(log)
            args_dfs.append(args_df)
        final_args_df = pd.concat(args_dfs, ignore_index=True)
        
        return(final_args_df)
    
    def get_request_event_list(self, log):
        '''  
        '''
        if 'Request get event list' in log:
            request_match = re.search(r'Request get event list: (.*?)}', log) 
            if request_match:
                request_string = request_match.group(1) + '}' 
                request_dict = ast.literal_eval(request_string)
                df = pd.json_normalize(request_dict)
                return df
                    
    def fetch_request_event_list(self, event_all_df):
        ''' 
        
        '''
        request_event_list_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0]  
            req_df = self.get_request_event_list(log)
            request_event_list_dfs.append(req_df)
        final_request_event_list_df = pd.concat(request_event_list_dfs, ignore_index=True)
        return(final_request_event_list_df)
            

    
    def get_delay_alarm(self, log):
        ''' 
        
        '''
        if 'Delay alarm event: ' in log: 
            delay_alarm_event_match = re.search(r'Delay alarm event: (.*?) s', log) 
            if delay_alarm_event_match:
                delay_alarm_event_string = delay_alarm_event_match.group(1) 
                return pd.DataFrame({'DelayAlarm': [delay_alarm_event_string]})
        
    def fetch_delay_alarm(self, event_all_df):
        ''' 
        
        '''
        delay_alarm_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0] 
            delay_alarm_event_df = self.get_delay_alarm(log)
            delay_alarm_dfs.append(delay_alarm_event_df) 
        final_delay_alarm_df = pd.concat(delay_alarm_dfs, ignore_index=True)
        return final_delay_alarm_df


    
    def get_time_query_event(self, log):
        ''' 
        
        '''
        if 'Time query event: ' in log:
            time_query_event_match = re.search(r'Time query event: (\d+\.\d{3})', log) 
            if time_query_event_match:
                time_query_event_string = time_query_event_match.group(1) 
                return pd.DataFrame({'TimeQueryEvent': [time_query_event_string]})
            else:
                return pd.DataFrame({'TimeQueryEvent': ['Not Found'],
                        })
        else:
            return pd.DataFrame({'TimeQueryEvent': ['Not Found'],
                        })

    def fetch_time_query_event(self, event_all_df):
        ''' 
        
        '''
        time_query_event_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0]  
            time_query_event_df = self.get_time_query_event(log)
            time_query_event_dfs.append(time_query_event_df) 
        final_time_query_event_df = pd.concat(time_query_event_dfs, ignore_index=True)
        return(final_time_query_event_df)
            
    def get_time_process_home_face(self, log):
        '''
         
        '''
        if 'Time query event: ' in log:
            time_time_process_home_face_match = re.search(r'Time query event: (\d+\.\d{3})', log) 
            if time_time_process_home_face_match:
                time_time_process_home_face_string = time_time_process_home_face_match.group(1)
                return pd.DataFrame({'TimeHomeFace': [time_time_process_home_face_string]})
            else:
                return pd.DataFrame({'TimeHomeFace': ['Not Found'],
                                    })
        else:
            return pd.DataFrame({'TimeHomeFace': ['Not Found'],
                        })
        
    def fetch_time_process_home_face(self, event_all_df):
        '''  
        '''
        time_process_home_face_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0]  
            time_process_home_face_df = self.get_time_process_home_face(log)
            time_process_home_face_dfs.append(time_process_home_face_df) 
        final_time_process_home_face_df = pd.concat(time_process_home_face_dfs, ignore_index=True)
        return final_time_process_home_face_df #df
    
    def get_all_time_worker(self, log):
        ''' 
        '''
        if 'All time worker' in log:
            all_time_worker_match = re.search(r"All time worker: (.*?) s" , log)
            if all_time_worker_match:
                all_time_worker_string = all_time_worker_match.group(1) 
                df = pd.DataFrame({'AllTimeWorker': [all_time_worker_string]})
                return df
            else:
                pass
                return pd.DataFrame({
                        'AllTimeWorker': ['Not Found'],
                        })    
        else:
            pass
            return pd.DataFrame({
                        'AllTimeWorker': ['Not Found'],
                        })
        
        
        

    def fetch_all_time_worker(self, event_all_df):
        ''' 
        
        '''
        all_time_worker_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0]  

            all_time_worker_event_df = self.get_all_time_worker(log)
            all_time_worker_dfs.append(all_time_worker_event_df) 
        final_all_time_worker_df = pd.concat(all_time_worker_dfs, ignore_index=True)
        return final_all_time_worker_df
        
        
    def get_response_alarm(self, log):
        if 'Response get event list: ' in log: 
                response_match = re.search(r" 'data': (.*?)'IsFinished': '1'}}" , log)
                if response_match:
                    response_string = response_match.group(1) + '}' 
                    response_string = response_string.replace("'", '"')
                    response_string = re.sub(r'"recface":\s*"({.*?})"', r'"recface": \1', response_string)
                    response_string = re.sub(r',\s*([\]})])', r'\1', response_string)
                    response_dict = ast.literal_eval(response_string)
                    df = pd.json_normalize(
                        response_dict, 
                        record_path=  ['AlarmArray'],
                        meta = ['SerialNumber'],
                    )
                    return df
        

    def fetch_response_alarm(self, event_all_df):
        response_event_dfs = []
        for log in ((event_all_df)):
            if isinstance(log, list):
                log = log[0] 
            response_event_df = self.get_response_alarm(log)
            response_event_dfs.append(response_event_df)
        final_response_event_df = pd.concat(response_event_dfs, ignore_index=True)
        return final_response_event_df

    
    




if __name__ == '__main__':   
    limit_memory(12 * 1024 * 1024 * 1024) 

    extractor = Extractor()
    extractor.write_all('json_logs')
    pass

