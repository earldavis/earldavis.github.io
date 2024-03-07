---
layout: post
title: Reading and Writing PyArrow
date: 2024-03-06 13:42
categories: [data_science,]
description: A post about reading and writing data using PyArrow format.
---

# Reading and Writing Using PyArrow

Import the packages and whatnot that you neet and initialize some paths if you want.

```python
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
import os

pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)
pd.set_eng_float_format(accuracy=3, use_eng_prefix=True)

xls_path = os.path.join(root, 'data', 'crappy-excel-formatted-data.xlsx')
print(root)
```

We are often confronted with data in an inconvenient format like Excel. The following block of code will convert each sheet into a separate `/.csv` file for easier processing.
this initial step requires the use of Pandas as I could not find a PyArrow function to read Excel and spit out the contents of the worksheets within.

```python
with pd.ExcelFile(xls_path) as xls:
    # print(xls.sheet_names)
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, dtype='object').astype('string')
        raw_path = os.path.join(root, 'data', 'raw', f'{sheet}.csv')
        df.to_csv(raw_path)
```

Sometimes your data is delivered in an ugly format or with extraneous columns. The `DSS_STORE.csv` extract had a large number of empty columns that began with the string `Column`.
PyArrow will allow you to read in a csv-formatted table and drop columns. It will allow you to do many other things, especially things that are conducive to manipulation in a columnar format.

```python
for _root, _dirs, files in os.walk(os.path.join(root, 'data', 'raw')):
    for file in files:
        if (not _root.endswith('DSSJU1003e')) and file.endswith('.csv'):
            raw_path = os.path.join(_root, file)
            tbl = csv.read_csv(raw_path)
            if file in ['DSS_STORE.csv',]:
                drop_cols = [col for col in tbl.column_names if col.startswith('Column')]
                tbl = tbl.drop_columns(drop_cols)
            csv_path = os.path.join(root, 'data', 'csv', file)
            csv.write_csv(tbl, csv_path)
```

I could have probably just saved the csv-formatted tables to parquet, but the applicaiton that I am currently working with only reads csv.
I prefer working with parquet files, so I went ahead and saved them to parquet for possible future use.

```python
for _root, _dirs, files in os.walk(os.path.join(root, 'data', 'csv')):
    for file in files:
        if file.endswith('.csv'):
            csv_path = os.path.join(_root, file)
            tbl = csv.read_csv(csv_path)
            pq_path = os.path.join(root, 'data', 'parquet', f"{file.split('.')[0]}.parquet")
            pq.write_table(tbl, pq_path)
```