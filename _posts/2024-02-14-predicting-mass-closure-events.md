---
layout: post
title: Using Python to Extract Predictions from a VertexAI Model Endpoint
date: 2024-02-14 14:40
categories: [data_science,]
description: An example of how to use python to transform data and query a VertexAI endpoint based on that transformation, because data engineering is not as simple as the tutorials make it out to be.
---

This scripts transforms some data so that it can be passed to a VertexAI endpoint to return predicitons.

```python
from predictions import *
import pandas as pd
import numpy as np
import os, json, ast, sys, importlib

pd.options.display.max_rows = 100
pd.options.display.max_columns = 50
pd.options.display.precision = 5

os.environ['PROJECT_ID'] = "name-of-the-project-id"
os.environ['ENDPOINT_ID'] = "endpoint-id-number"
os.environ['LOCATION'] = 'us-central1'
os.environ['BUCKET'] = 'name-of-the-project-id-bucket'

credentials, project = google.auth.default() if "credentials" not in locals() or "project" not in locals() else (credentials, project)
location = os.getenv('LOCATION')
endpoint_id = os.getenv('ENDPOINT_ID')
home_path = "path/to/the/home/directory"

# ! gcloud auth application-default login
read_path = os.path.join(home_path, 'data', 'obfuscated-incidents.parquet')
df = pd.read_parquet(read_path, engine='pyarrow', filters=[
    ('ym_opened', 'in', ['2023-07', ]),
    ], columns=['number', 'opened_at', 'description', 'close_notes'])
df['close_notes'] = df['close_notes'].fillna('no notes')
df['description'] = df['description'].fillna('no description')

network_list = ['socket timeout', 'nic redundancy', 'ping failure', 'networkredundancylostalarm']
write_file='df.json'

df.loc[:, 'network_incident'] = df.loc[:, 'description'].str.lower().str.contains('|'.join(network_list), case=False, na=False)
df.loc[:, 'nagios_incident'] = df.loc[:, 'description'].str.lower().str.contains('nagios', case=False, na=False)
df.loc[:, 'autosys_incident'] = df.loc[:, 'description'].str.lower().str.contains('autosys', case=False, na=False)
df.loc[:, 'avg_count'] = (
    df[['opened_at', 'number']]
    .rolling(window=pd.Timedelta(seconds=60), center=True, on='opened_at', closed='both').count()
    .rolling(window=pd.Timedelta(seconds=900), center=True, on='opened_at', closed='both').max()
    .iloc[:, 1]
)
df.loc[:, 'log_avg_count'] = np.log1p(df.loc[:, 'avg_count'])

df = (
    df[['number', 'opened_at', 'close_notes']]
    .groupby('close_notes')
    .rolling(window=pd.Timedelta(seconds=900), center=True, on='opened_at', closed='both')
    .count().reset_index(0).rename(columns={'number':'max_close_count'})
    .merge(df, how='right', left_on=['close_notes', 'opened_at'], right_on=['close_notes', 'opened_at'])
    .drop_duplicates()
)

df.loc[:, 'scaled_log_avg_count'] = (df.loc[:, 'log_avg_count'] - np.mean(df.loc[:, 'log_avg_count'])) - np.std(df.loc[:, 'log_avg_count'])
df.loc[:, "autosys_storm"] = df.loc[:, 'autosys_incident'] * df.loc[:, 'scaled_log_avg_count']
df.loc[:, "nagios_storm"] = df.loc[:, 'nagios_incident'] * df.loc[:, 'scaled_log_avg_count']
df.loc[:, "network_storm"] = df.loc[:, 'network_incident'] * df.loc[:, 'scaled_log_avg_count']
df.to_json(write_file, lines=True, orient='records')
jsn = list()
with open(write_file, 'r') as f:
    for line in f:
        jsn.append(json.loads(line))
        
lst = map(process_func, jsn)
pdf = (
    pd.DataFrame.from_records(lst)
    .assign(**{'opened_at': lambda s: pd.to_datetime(s['opened_at'], unit='ms')})
)

write_json = os.path.join(home_path, 'data', 'predict-mass-close.json')
pdf.to_json(write_json, lines=True, orient='records')
_ = pd.read_json(write_json, lines=True, orient='records')
_.head()
```