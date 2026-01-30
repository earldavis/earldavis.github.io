#!/home/your_favorite_user/data-capture-38/bin/python
import pyarrow as pa
import pyarrow.parquet
import pandas as pd 
import numpy as np 
import os, requests, time, urllib3
from datetime import datetime, timedelta
from collections import defaultdict

apiKeyDict = {
    'server_0':'api_key_0',
    'server_1':'api_key_1',
    'server_2':'api_key_2',
    'server_3':'api_key_3',
    'server_4':'api_key_4',
    'server_5':'api_key_5',
    'server_6':'api_key_6',
    'server_7':'api_key_7'
}

def unix_time(time_string: datetime):
    _ = time_string.timetuple()
    return time.mktime(_)

def repartition(path, partition=None, partitions=None):
    _ = pa.parquet.read_table(path) if partition is None else pa.parquet.read_table(path, filters=[('ym_state_time', 'in', partition)])
    if partitions is None:
        pa.parquet.write_to_dataset(_, root_path=path, existing_data_behavior='delete_matching')
    else:
        pa.parquet.write_to_dataset(_, root_path=path, partition_cols=partitions, existing_data_behavior='delete_matching')
    return None

def get_request(server, apikey, start_time=datetime.min, end_time=datetime.max):
    unix_start, unix_end = (unix_time(start_time), unix_time(end_time))
    params = {'apikey':apikey, 'starttime':unix_start, 'endtime':unix_end, 'format':'json'}
    return requests.get(f'https://{server}.web.example.com/nagios/api/v1/objects/statehistory', params=params, verify=False) 

if __name__ == "__main__":
    # root = os.path.join('/data')
    # parquetPath = os.path.join(root, 'nagios', 'state-change-objects.parquet')
    root = os.path.join('C:\\', 'Users', 'your_favorite_user', 'Documents', 'nagios', 'data')
    parquetPath = os.path.join(root, 'state-change-objects.parquet')
    end_time = datetime.now()
    logPath = os.path.join(root, 'logs', f"log{end_time.strftime('%Y%m%d')}.txt")
    errorLogPath = os.path.join(root, 'logs', f"errorlog{end_time.strftime('%Y%m%d')}.txt")
    # start_time = end_time - timedelta(days=1, minutes=10)
    for _root, _dir, _file in os.walk(parquetPath):
        if len(_dir) > 0:
            _dir.sort()
            part = [d.split('=')[1] for d in _dir[-2:]]

    _0 = pa.parquet.read_table(parquetPath, filters=[('ym_state_time', 'in', part)])
    _1 = _0.to_pandas()
    _2 = (
        pd.DataFrame(_1)
        .groupby('nagios_server')
        .agg(**{'max_time':pd.NamedAgg('state_time', np.max)}))
    _2['max_time'] = _2['max_time'].apply(datetime.strptime, args=('%Y-%m-%d %H:%M:%S', ))
    
    finishTimeDict = defaultdict(list)
    for server, finish_time in _2.itertuples(index=True):
        datetime_list = list()
        end_time = pd.to_datetime(datetime.now())
        start_time = end_time
        if start_time < finish_time:
            with open(errorLogPath, 'a') as f:
                f.write(f"{datetime.now()}|{server}: Start Time {start_time} less than Finish Time {finish_time}.")
            continue
        while start_time >= finish_time: 
            start_time = end_time - timedelta(days=1)
            datetime_list.append((max(start_time, finish_time), end_time))
            end_time = start_time
        finishTimeDict[server] = datetime_list
        
    urllib3.disable_warnings()
    for server in apiKeyDict.keys():
        tic = time.perf_counter()
        if (finishTimeDict[server] is None) or (len(finishTimeDict[server]) <= 0): continue
        for start, stop in finishTimeDict[server]:
            apikey = apiKeyDict[server]
            nDF = get_request(server, apikey, start_time=start, end_time=stop)
            df = pd.DataFrame(nDF.json()['stateentry'])
            try:
                _ = df.assign(**{
                    'nagios_server': server,
                    'ym_state_time': lambda d: pd.to_datetime(d['state_time']).dt.strftime('%Y-%m')
                    })
                row, column = _.shape
                if row >= 100000:
                    with open(errorLogPath, 'a') as f:
                        f.write(f"{datetime.now()}|{server}: More than 100_000 rows returned.\n")
                if column != 16: 
                    with open(errorLogPath, 'a') as f:
                        f.write(f"{datetime.now()}|{server}: Wrong column count.\n")
                tbl = pa.Table.from_pandas(_)
                pa.parquet.write_to_dataset(tbl, root_path=parquetPath, partition_cols=['ym_state_time', ])
            except KeyError as err:
                if len(df.columns) <= 0: 
                    continue
                else:
                    with open(errorLogPath, 'a') as f:
                        f.write(f"{datetime.now()}|KeyError:{server}:{err}|{df.columns.to_list()}\n")
            
        toc = time.perf_counter()
        with open(logPath, 'a') as f:
            f.write(f"{datetime.now()}|{server}|{toc - tic:0.4f}s\n")

