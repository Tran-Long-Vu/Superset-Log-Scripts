import os
import pandas as pd
import json
import numpy as np
import glob
import re
import tqdm as tqdm
import ast
from pathlib import Path


class Extractor():
    def __init__(self) -> None:
        self.path_to_json = './json_logs'
        self.values_df_rev = self.read_json() 
        self.event_data_log_string = self.collect_all_events(self.values_df_rev) 
        pass
    
    def read_json(self):
        '''   
        Input: path to json file
        Output: a Series of all logs in reverse order.
        '''
        temp_df = pd.DataFrame()
        json_pattern = os.path.join(self.path_to_json,'*.json')
        file_list = glob.glob(json_pattern)
        print(str(len(file_list)))
        dfs = [] 
        for file in tqdm.tqdm(enumerate(file_list), total=len(file_list), desc="Finding files"):
            
            data = pd.read_json(file[1])
            dfs.append(data) 
        temp_df = pd.concat(dfs) 
        result_column_df = temp_df['data']['result']
        all_values = []
        for i in range(len(result_column_df)): 
            values = temp_df['data']['result'][i][0]['values']  
            all_values.extend(values)  
        values_df = pd.Series(all_values)
        values_df_rev = values_df.iloc[::-1]
        return values_df_rev

    def collect_all_events(self, series):
        '''  
        Input: series variable of all read logs.
        Output: a dataframe with events as data points in string format.
        '''
        start_indices = []
        end_indices = []
        for i, value in tqdm.tqdm(enumerate(series), total=len(series), desc="Finding indices"):
            if 'Start fetch event' in str(value):
                start_indices.append(i)
            elif '======END======' in str(value):
                end_indices.append(i)  
        log_segments = []
        end_index = 0 

        for start in tqdm.tqdm(start_indices, desc="Gathering log segments"):
            while end_index < len(end_indices) and end_indices[end_index] <= start:
                end_index += 1
            if end_index < len(end_indices):
                end = end_indices[end_index]
                segment = ' '.join(str(series[i]) for i in range(start + 1, end))
                log_segments.append(segment)
        event_data_df = pd.DataFrame(log_segments, columns=['Log String'])
        event_data_df.index.name = 'Index'  
        event_data_log_string = event_data_df['Log String']
        return event_data_log_string  
    
    def get_args(self,log):
        '''  
        input: log strings in JSON
        output: arg data fields as dataFrame for single data point
        '''
        if 'arg: {' in log:
            arg_match = re.search(r'arg:\s*(\{.*?\})', log)
            if arg_match:
                args_string = arg_match.group(1)
                args_dict = ast.literal_eval(args_string) ##
                df = pd.json_normalize(args_dict)
                return df  
            else:
                
                return pd.DataFrame({'sn': ['Not Found'],
                                    'user_id': ['Not Found'],
                                    'token': ['Not Found'],
                                    'time_millis': ['Not Found'], 
                                    'encrypted_str': ['Not Found'], 
                                    'time_query_latest': ['Not Found'], 
                                    'datetime': ['Not Found']})
        else:
            return pd.DataFrame({'sn': ['Not Found'],
                                'user_id': ['Not Found'],
                                'token': ['Not Found'], 
                                'time_millis': ['Not Found'],
                                'encrypted_str': ['Not Found'],
                                'time_query_latest': ['Not Found'], 
                                'datetime': ['Not Found']})
            
        
        
    def fetch_args(self):
        '''  
        input: 
        output: dataFrame of all event args
        '''
        args_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding args"):
            log = str(log)
            if isinstance(log, list):
                log = log[0]
            args_df = self.get_args(log)
            args_dfs.append(args_df) # Null error
        final_args_df = pd.concat(args_dfs, ignore_index=True)
        
        return(final_args_df)
    
    def get_request_event_list(self,log):
        '''  
        
        '''
        if 'Request get event list' in log:
            request_match = re.search(r'Request get event list: (.*?)}', log) 
            if request_match:
                request_string = request_match.group(1) + '}' 
                request_dict = ast.literal_eval(request_string)
                df = pd.json_normalize(request_dict)
                return df  
            else:
                return pd.DataFrame({'sn': ['Not Found'],
                                    'startTime': ['Not Found'],
                                    'endTime': ['Not Found'],
                                    })
                
        else:
            return pd.DataFrame({'sn': ['Not Found'],
                        'startTime': ['Not Found'],
                        'endTime': ['Not Found'],
                        })
            

        
    def fetch_request_event_list(self):
        ''' 
        
        '''
        request_event_list_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding requests"):
            if isinstance(log, list):
                log = log[0]  
            req_df = self.get_request_event_list(log)
            request_event_list_dfs.append(req_df)
        final_request_event_list_df = pd.concat(request_event_list_dfs, ignore_index=True)
        return(final_request_event_list_df)
        

    
    def get_delay_alarm(self,log):
        ''' 
        
        '''
        if 'Delay alarm event: ' in log: 
            delay_alarm_event_match = re.search(r'Delay alarm event: (.*?) s', log) 
            if delay_alarm_event_match:
                delay_alarm_event_string = delay_alarm_event_match.group(1) 
                return pd.DataFrame({'DelayAlarm': [delay_alarm_event_string]})
            else:
                return pd.DataFrame({'DelayAlarm': ['Not Found'],
                                    })
        else:
            return pd.DataFrame({'DelayAlarm': ['Not Found'],
                        })
        
        
    
    
        
    def fetch_delay_alarm(self):
        ''' 
        
        '''
        delay_alarm_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding delay alarm"):
            if isinstance(log, list):
                log = log[0] 
            delay_alarm_event_df = self.get_delay_alarm(log)
            delay_alarm_dfs.append(delay_alarm_event_df) 
        final_delay_alarm_df = pd.concat(delay_alarm_dfs, ignore_index=True)
        return final_delay_alarm_df
    
    def get_time_query_event(self,log):
        ''' 
        
        '''
        if 'Time query event: ' in log:
            time_query_event_match = re.search(r'Time query event: (\d+\.\d{3})', log) # search string & end string 
            if time_query_event_match:
                time_query_event_string = time_query_event_match.group(1) 
                return pd.DataFrame({'TimeQueryEvent': [time_query_event_string]})
            else:
                return pd.DataFrame({'TimeQueryEvent': ['Not Found'],
                        })
        else:
            return pd.DataFrame({'TimeQueryEvent': ['Not Found'],
                        })

    def fetch_time_query_event(self):
        ''' 
        
        '''
        time_query_event_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding time query event"):
            if isinstance(log, list):
                log = log[0]  
            time_query_event_df = self.get_time_query_event(log)
            time_query_event_dfs.append(time_query_event_df) 
        final_time_query_event_df = pd.concat(time_query_event_dfs, ignore_index=True)
        return(final_time_query_event_df)
            
    def get_time_process_home_face(self,log):
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
        
    def fetch_time_process_home_face(self):
        '''  
        '''
        time_process_home_face_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding time process home face"):
            if isinstance(log, list):
                log = log[0]  
            time_process_home_face_df = self.get_time_process_home_face(log)
            time_process_home_face_dfs.append(time_process_home_face_df) 
        final_time_process_home_face_df = pd.concat(time_process_home_face_dfs, ignore_index=True)
        return final_time_process_home_face_df #df
    
    def get_all_time_worker(self,log):
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
        
        
    

    def fetch_all_time_worker(self):
        ''' 
        
        '''
        all_time_worker_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding all time worker"):
            if isinstance(log, list):
                log = log[0]  

            all_time_worker_event_df = self.get_all_time_worker(log)
            all_time_worker_dfs.append(all_time_worker_event_df) 
        final_all_time_worker_df = pd.concat(all_time_worker_dfs, ignore_index=True)
        return final_all_time_worker_df
        
        
    def get_response_alarm(self,log):
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
                        meta = [['SerialNumber'],
                                ['AlarmTotal']]                
                    )
                    return df
                else: 
                    return pd.DataFrame({
                            'SerialNumber': ['Not Found'],
                            'AlarmTotal': ['Not Found'],
                            
                            'AlarmEvent': ['Not Found'],
                            'AlarmId': ['Not Found'],
                            'AlarmMsg': ['Not Found'],
                            'AlarmTime': ['Not Found'],
                            'Channel': ['Not Found'],
                            
                            'PicInfo.ObjName': ['Not Found'],
                            'PicInfo.ObjSize': ['Not Found'],
                            'PicInfo.StorageBucket': ['Not Found'],
                            'VideoInfo.VideoLength': ['Not Found'],
                            'PicErr': ['Not Found'],
                            })    
        else:
            return pd.DataFrame({
                        'SerialNumber': ['Not Found'],
                        'AlarmTotal': ['Not Found'],
                        
                        'AlarmEvent': ['Not Found'],
                        'AlarmId': ['Not Found'],
                        'AlarmMsg': ['Not Found'],
                        'AlarmTime': ['Not Found'],
                        'Channel': ['Not Found'],
                        
                        'PicInfo.ObjName': ['Not Found'],
                        'PicInfo.ObjSize': ['Not Found'],
                        'PicInfo.StorageBucket': ['Not Found'],
                        'VideoInfo.VideoLength': ['Not Found'],
                        'PicErr': ['Not Found'],
                        })
        
    
    def fetch_response_alarm(self):
        response_event_dfs = []
        for log in tqdm.tqdm((self.event_data_log_string), total=len(self.event_data_log_string), desc="Finding alarm response"):
            if isinstance(log, list):
                log = log[0] 
            response_event_df = self.get_response_alarm(log)
            response_event_dfs.append(response_event_df)
        final_response_event_df = pd.concat(response_event_dfs, ignore_index=True)
        return final_response_event_df

    
    def fetch_all(self):
        ''' 
        
        
        '''
        df_result = pd.concat(
                        [
                        self.fetch_args(),
                        self.fetch_request_event_list(),
                        self.fetch_time_query_event(),
                        self.fetch_time_process_home_face(),
                        ],
                        # ignore_index=True,
                        axis=1)
        
        alarm_df = pd.concat(
                        [
                        self.fetch_response_alarm(),
                        self.fetch_delay_alarm()
                        ],
                        # ignore_index=True,
                        axis=1)
        return df_result , alarm_df




if __name__ == '__main__':   
    print('Extracting')
    extractor = Extractor()
    df_result , alarm_df = extractor.fetch_all() 
    
    print(df_result)
    print(alarm_df)
    
    df_result.to_csv('./output_csv/event_log_data.csv')
    alarm_df.to_csv('./output_csv/alarm_data.csv')
    
    print('Data converted to CSV.')
    pass


