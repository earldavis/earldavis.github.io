---
layout: post
title: How to Read Really Big Files that May Not Fit In Memory
date: 2024-02-22 14:06
categories: [data_science,]
description: A post about reading reading very large CFPB mortgage data files.
---

First you need to convert the zipfile, in this case a 1.6GB zipped archive, into something that is unzipped.
This chunk of code converts into an almost 20GB csv file. This file will likely crash your computer if you 
try to open it with Excel.

```python
import os, zipfile

# Specify the path to the zip file
zip_path = 'hmda_2007_nationwide_all-records_labels.zip'

# Open the zip file
with zipfile.ZipFile(zip_path, 'r') as zip_file:
    # Extract the contents of the zip file
    zip_file.extractall('./hmda')
```

## Use Pandas and PyArrow instead.

```python
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os, zipfile

pd.options.display.max_rows = 100
pd.options.display.max_columns = 50
pd.options.display.precision = 3

print(os.getcwd())
file_name = 'hmda_2007_nationwide_all-records_labels'
test_name = 'hmda_2007_1M'
```

Specify the path to the extracted CSV file and create a test file to try out how big to make your chunks and partitions.

```python
csv_path = f'./hmda/{test_name}.csv'
pd.read_csv(csv_path, iterator=False, dtype=str, nrows=1_000_000).to_csv('./hmda/hmda_2007_1M.csv')
```

The first line allows you to run the test. Commentted out it runs the entire dataset.
Create some variables to hold parameters.

```python
# file_name = test_name
csv_path = f'./hmda/{file_name}.csv'
parquet_path = f'./{file_name}.parquet'
chunk_size = 1_000_000
partition_count = 10
```

Read the CSV file into a pandas dataframe in chunks of 1M rows. As long as the chunks are greater than 0, 
you create a partition key by converting the `respondent_id` into a long integer and taking the modulus
with respect to the number of partitions you want. I did 10. I converted the entire dataframe to a string type
for convenience, as I was more curious about how you process a big data file without a Spark cluster on a laptop.
I saved the file to parquet format using the `partition_key` as the partition column. Read another chunk at the end 
to initialize the process all over again. The iterator will throw a `StopIteration` exception when finished.

```python
with pd.read_csv(csv_path, iterator=True, dtype=str) as reader: 
    try:
        df = reader.get_chunk(chunk_size)
        while df.shape[0] > 0:
            df['partition_key'] = df['respondent_id'].str.replace(r'\D', '', regex=True).astype(np.int64) % partition_count
            df = df.astype(str)
            df.to_parquet(parquet_path, engine='pyarrow', partition_cols=['partition_key']) # , existing_data_behavior='delete_matching')
            df = reader.get_chunk(chunk_size)
    except StopIteration as e:
        print(e)
```

To count the total number of rows, create a list to hold the values, read the file by partition, capture the row count (`df.shape[0]`), 
and finally sum the list.

```python
n_list = list()
for n in range(partition_count):
    df = pq.read_table(parquet_path, filters=[('partition_key', 'in', [n, ])]).to_pandas()
    n_list.append(df.shape[0])
sum(n_list)
```

You will get 26_605_695 rows of data.