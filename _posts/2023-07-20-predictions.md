---
layout: post
title: Getting Predictions from Vertex AI using Python
date: 2023-07-20 15:56
categories: [data_science,]
description: Functions for extracting predictions from Vertex AI endpoints.
---

Functions for extracting the predictions from Vertex AI. There is some code to attempt to do it in parallel. Mostly used for the `predict` and `process_func` functions:

```python
import os, ast, argparse, json
import pandas as pd
from typing import Dict, List, Union
from multiprocessing import Pool
import google.auth
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

os.environ['PROJECT_ID'] = "the-project-id"
os.environ['ENDPOINT_ID'] = "big-int-that-is-the-endpoint-id"
os.environ['LOCATION'] = 'us-central1'
os.environ['BUCKET'] = 'the-project-id-bucket'

credentials, project = (credentials, project) if "credentials" in globals() and "project" in globals() else google.auth.default()
location = os.getenv('LOCATION')
endpoint_id = os.getenv('ENDPOINT_ID')

aiplatform.init(project=project, location=location, staging_bucket=f"gs://{os.getenv('BUCKET')}", credentials=credentials)
client = aiplatform.gapic.PredictionServiceClient(client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"})
endpoint = client.endpoint_path(project=project, location=location, endpoint=endpoint_id)

def predict(instances: Union[Dict, List[Dict]], client=client, endpoint=endpoint):
    """
    `instances` can be either single instance of type dict or a list
    of instances.
    """
    instances = instances if type(instances) == list else [instances]
    instances = [json_format.ParseDict(instance_dict, Value()) for instance_dict in instances]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    response = client.predict(endpoint=endpoint, instances=instances, parameters=parameters)
    # The predictions are a google.protobuf.Value representation of the model's predictions.
    predictions = response.predictions
    return predictions
        
def process_func(j):
    try:
        lbl = [dict(p) for p in predict(
            {
             # 'avg_count':j.get('avg_count'), 
             'network_incident':j.get('network_incident'), 
             'nagios_incident':j.get('nagios_incident'), 
             'autosys_incident':j.get('autosys_incident'),
             'scaled_log_avg_count':j.get('scaled_log_avg_count'), 
             'autosys_storm':j.get('autosys_storm'), 
             'nagios_storm':j.get('nagios_storm'), 
             'network_storm':j.get('network_storm')}, 
            client, endpoint)][0]['predicted_label'][0]
    except Exception as err:
        raise Exception(err)
    j['predicted_values'] = ast.literal_eval(lbl.capitalize())
    return j

def process_pool(df):
    with Pool(8) as p:
        project = google.auth.default()[1]
        location = os.getenv('LOCATION')
        endpoint_id = os.getenv('ENDPOINT_ID')
        try:
            client = aiplatform.gapic.PredictionServiceClient(client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"})
            endpoint = client.endpoint_path(project=project, location=location, endpoint=endpoint_id)
        except Exception as err:
            raise Exception(err)
        lst = p.map_async(process_func, df)
    return lst

if __name__ == '__main__':
    __spec__ = None
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    args = parser.parse_args()
    read_file = args.file
    jsn = list()
    with open(read_file, 'r') as f:
        for line in f:
            _ = json.loads(line)
            jsn.append(_)
    process_pool(jsn)
```