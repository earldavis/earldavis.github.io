##############!/net/ent-home/home/p/pb471f/data-capture-38/bin/python
import pyarrow as pa
import pyarrow.parquet
import pandas as pd 
import numpy as np 
import os, requests, time, urllib3
from datetime import datetime, timedelta

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

def unix_time(time_string):
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
    root = os.path.join('C:\\', 'Users', 'your_favorite_user', 'Documents', 'nagios', 'data')
    parquetPath = os.path.join(root, 'state-change-objects.parquet')
    end_time = datetime.now()
    logPath = os.path.join(root, 'logs', f"log{end_time.strftime('%Y%m%d%H%M%S')}.txt")
    errorLogPath = os.path.join(root, 'logs', f"errorlog{end_time.strftime('%Y%m%d%H%M%S')}.txt")
    
    for _root, _dir, _file in os.walk(parquetPath):
        if len(_dir) > 0:
            part = max(_dir).split('=')[1]

    _0 = pa.parquet.read_table(parquetPath, filters=[('ym_state_time', 'in', [part, ])])
    _1 = _0.to_pandas()
    _2 = pd.DataFrame(_1)['state_time']
    _3 = np.max(_2)
    finish_time = datetime.strptime(_3, '%Y-%m-%d %H:%M:%S')
    
    datetime_list = list()
    start_time = end_time
    while start_time >= finish_time: 
        start_time = end_time - timedelta(hours=1)
        datetime_list.append((max(start_time, finish_time), end_time))
        end_time = start_time
    
    urllib3.disable_warnings()
    for start, stop in datetime_list:
        for server in apiKeyDict.keys():
            apikey = apiKeyDict[server]
            tic = time.perf_counter()
            nDF = get_request(server, apikey, start_time=start, end_time=stop)
            df = pd.DataFrame(nDF.json()['stateentry'])
            if len(df.columns) <= 0: 
                continue
            try:
                _ = df.assign(**{
                    'nagios_server': server,
                    'ym_state_time': lambda d: pd.to_datetime(d['state_time']).dt.strftime('%Y-%m')
                    })
                row, column = _.shape
                assert row < 100000, 'Too many rows returned.'
                assert column == 16, 'Wrong column count'
                tbl = pa.Table.from_pandas(_)
                pa.parquet.write_to_dataset(tbl, root_path=parquetPath, partition_cols=['ym_state_time', ])
                toc = time.perf_counter()
                with open(logPath, 'a') as f:
                    f.write(f"{datetime.now()}|{server}|{start.strftime('%Y%m%d%H%M%S')}|{_.shape}|{toc - tic:0.4f}s\n")
            except KeyError as err:
                with open(errorLogPath, 'a') as f:
                    f.write(f"{datetime.now()}|KeyError:{start.strftime('%Y%m%d%H%M%S')}|{server}:{err}|{df.columns.to_list()}\n")
            except AssertionError as err:
                with open(errorLogPath, 'a') as f:
                    f.write(f"{datetime.now()}|AssertionError:{start.strftime('%Y%m%d%H%M%S')}|{server}:{err}\n")
            toc = time.perf_counter()
            print(toc - tic, )

    # for _root, _dir, _files in os.walk(parquetPath):
    #     for d in _dir:
    #         part = [p.split('=')[1] for p in [d, ]]
    #         repartition(parquetPath, partition=part, partitions=['ym_state_time', ])

