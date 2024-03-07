---
layout: post
title: Databricks Tutorial Using EIA Data
date: 2022-03-29 00:00
categories: [data_science,]
description: A demonstation of how to use Databricks, PySpark, Koalas (Pandas), SQL. and some other tools
---

#### Initialling Some Useful Values
You can mount Azure ADLS or blob storage. Here we are setting the `storage_account_name`, `container_name`, `file_location`, `conf_key`, and `mount_point` to mount an Azure Blob Storage Container later. I am also unmounting storage for demonstation purposes.

```python
storage_account_name = "my_storage_acct"
container_name = "electricity-data"

file_location = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}"
```

```python
if mount_point in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.unmount(mount_point)
```

```python
def PrintList(_list): 
    [print(mnt) for mnt in _list]
    print()

PrintList(dbutils.fs.mounts())
# dbutils.fs.mounts()[0].mountPoint
```

#### Creating Secrets
In order to mount storage, you should create and store secrets to access the remote storage keys or SAS values. To create a secret in a __premium__ databricks subscription: 

[https://\<URL_PREFIX\>.azuredatabricks.net/#secrets/createScope](https://adb-6822376287653902.2.azuredatabricks.net/#secrets/createScope)

There is a utility module called `dbutils` that allows for interacting with the driver on the cluster and Azure Databricks Workspace. Using `dbutils` we can 'list' and 'get' secrets from the Azure Databricks workspace. There is no 'set' method available. The 'set' method is available as part of the REST api. The secrets we are accessing are part of Azure Key Vault, Azures key-value storage for sensitive ('secret') values.

```python
scope_name = "electricity-scope"
print(dbutils.secrets.list(scope_name))
```

#### Secrets Stay Secret
Secrets cannot be revealed via a print function.

```python
print(dbutils.secrets.get(scope = scope_name, key = "storage-account-access-key"))
```

#### Setting Remote Object Settings via the Spark Configuration
You do not have to mount storage. You can reference remote object storage within the notebook, but it not stored in the Azure Databricks Workspace.

For reference from above:

```python
storage_account_name = "my_storage_acct"
container_name = "electricity-data"

file_location = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
mount_point = f"/mnt/{container_name}"
```

```python
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    dbutils.secrets.get(scope = scope_name, key = "storage-account-access-key")
)
```

```python
PrintList(dbutils.fs.ls(file_location))
```

#### Mounting Storage
Rather than setting the object storage path each time, we can mount storage that will be preserved across Azure Databricks Workspace sessions.

```python
dbutils.fs.mount(
  source = file_location,
  mount_point = mount_point,
  extra_configs = {f"{conf_key}":dbutils.secrets.get(scope = scope_name, key = "storage-account-access-key")}
)
```

Below we reference the mount point with respect to the dbfs root: `/mnt/electricity-data`. 

```python
[print(
    f"""Mount Point:\t{mnt.mountPoint}
    Source:\t\t{mnt.source}
    """
    ) for mnt in dbutils.fs.mounts() if mnt.mountPoint in [mount_point, '/']]
print()
```

#### Useful Modules
Next we import some useful modules: OS, Numpy, Koalas, and pyspark's SQL functions.

```python
import os
import numpy as np
import databricks.koalas as ks
from pyspark.sql.functions import *

```
```python
dbutils.fs.mkdirs(os.path.join(mount_point, 'bronze'))
dbutils.fs.ls(mount_point + '/bronze')
print(f"Raw Files:")
[print("Path:",f.path,"\tSize:",f.size) for f in dbutils.fs.ls(mount_point)]
print()
print(dbutils.fs.ls(mount_point)[4])
```

```python
file_list = dbutils.fs.ls(mount_point)
file_location = "dbfs:/mnt/electricity-data/f923_2011.csv"
write_location = file_list[0].path + file_location.split('/')[-1].split('.')[0]

print(f"Read Location: {file_location}"); print(f"Write Location: {write_location}")
```

#### Reading in a DataFrame
We can read in a csv file using Koalas. Koalas is a API whose syntax is close to Pandas, but is designed for distributed computing on DataFrames in Spark. Pandas on Spark has the limitation of only working on one node. If the data becomes big, sufficiently partitioned to no longer be contained on a single node, then Pandas will fail. Koalas overcomes this limitation.

Below we are extracting a single csv file to form the schema in SQL DDL format.

```python
df = ks.read_csv("dbfs:/mnt/electricity-data/f923_2011.csv", header=0) # file_location, header=0)
df.columns = [s.replace(" ", "_").replace("&", "and").replace(".", "_") for s in df.columns]
df.columns = [s.replace(r"(", r"").replace(r")", r"") for s in df.columns]
print(df.spark.schema(), '\n\n', df.spark.schema().simpleString())
ddl = [df.spark.schema().simpleString().split("<")[1].split(">")[0]]
ddl = [s.replace(':', ' ') for s in ddl][0].lower()
print("\n")
print(ddl)
```

```python
print(dbutils.fs.ls(mount_point + "/bronze"))
dbutils.fs.mkdirs(os.path.join(mount_point, 'silver'))
dbutils.fs.mkdirs(os.path.join(mount_point, 'gold')); print()
```

```python
for path in [f.path for f in dbutils.fs.ls(mount_point) if not f.isDir()]:
    name = path.split('/')[-1].split('.')[0]
    write_location = f"dbfs:/mnt/electricity-data/bronze/{name}"
    df = ks.read_csv(path, header=None, names=ddl) # if df.columns[-1].lower() != 'year' else ks.read_csv(path, header=0)
#     df = spark.read.format('csv').schema(ddl).option("header", False).load(path)
#     print(f"header length {name}: {df.columns.size}")
    assert df.columns.size == 97, f"{name} has {df.columns.size} columns"
    assert df.columns[-1].lower() == 'year', f"{name} last field should be 'year'"
    try:
        df.to_delta(write_location, mode='overwrite')
    except Exception as err:
        print(f"{err} in {path.split(r'/')[-1]}")

```

```python
for f in dbutils.fs.ls(mount_point):
    if f.isDir():
        print(f.path)
print('\n')
[print(f.path) for f in dbutils.fs.ls(os.path.join(mount_point, 'bronze', 'f923_2020'))]
print()
```

```python
silver_path = os.path.join(mount_point, 'silver')
```

```python
spark.conf.set('spark.databricks.delta.formatCheck.enabled', 'false')
bdf = ks.DataFrame()
location = f"{mount_point}/silver/generation_and_fuel_data"
for yr in range(2008, 2022):
    df = ks.read_delta(os.path.join(mount_point, 'bronze', 'f923_' + str(yr)))
    df = df.loc[df['plant_id'].notnull(), :]
    df = df.drop(['operator_id'], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("reserved")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("netgen_")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("elec_mmbtu")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("tot_mmbtu")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("mmbtuper_unit_")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("elec_quantity_")], axis=1)
    df = df.drop(df.columns[df.columns.str.startswith("quantity_")], axis=1)
    for var in ['plant_id', 'nuclear_unit_id', 'naics_code', 'eia_sector_number', 'year']:
        df[var] = df[var].astype("string").astype("int")
    bdf = bdf.append(df, ignore_index=True)
#     write_path = mount_point + '/silver/f923_ytd_' + str(yr)
#     df.to_delta(write_path, mode='overwrite')

bdf.to_delta(location, mode='overwrite', partition_cols='year')
spark.conf.set('spark.databricks.delta.formatCheck.enabled', 'true')
```

```sql
SELECT
  COUNT(*)
FROM
  delta.`dbfs:/mnt/electricity-data/silver/generation_and_fuel_data`
WHERE
  year <= 2008;
```

```python
DDL = bdf.spark.schema().simpleString().split('<')[-1].split('>')[0].replace(':', ' ')
DDL
```

```sql
DROP TABLE IF EXISTS f923.generation_and_fuel_data;
DROP TABLE IF EXISTS f923.percent_consumption;
DROP DATABASE f923;
```

```python
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS f923 LOCATION '{mount_point}'
""")
```

```python
spark.sql(f"""
CREATE TABLE f923.generation_and_fuel_data ({DDL}) 
USING DELTA 
PARTITIONED BY (year) LOCATION '{location}'
""")
```

```python
df = spark.sql(f"""
SELECT
    Year,
    State,
    Net_Generation,
    ROUND(Electric_Fuel_Consumption/Total_Fuel_Consumption, 3) AS Percent_Electric_Consumption
FROM (
    SELECT
        year AS Year,
        state AS State,
        ROUND(SUM(net_generation_megawatthours), 1) AS Net_Generation,
        SUM(total_fuel_consumption_quantity) AS Total_Fuel_Consumption,
        SUM(electric_fuel_consumption_quantity) AS Electric_Fuel_Consumption
    FROM f923.generation_and_fuel_data
    WHERE State IS NOT NULL AND Year IS NOT NULL
    GROUP BY Year, State
)
WHERE ROUND(Electric_Fuel_Consumption/Total_Fuel_Consumption, 3) BETWEEN 0.1 AND 0.9
ORDER BY State, Year DESC
""")
display(df)
```

```sql 
CREATE OR REPLACE TABLE f923.percent_consumption AS (
    SELECT
      Year,
      State,
      Net_Generation,
      ROUND(Electric_Fuel_Consumption / Total_Fuel_Consumption, 3) AS Percent_Electric_Consumption
    FROM
      (
        SELECT
          year AS Year,
          state AS State,
          ROUND(SUM(net_generation_megawatthours), 1) AS Net_Generation,
          SUM(total_fuel_consumption_quantity) AS Total_Fuel_Consumption,
          SUM(electric_fuel_consumption_quantity) AS Electric_Fuel_Consumption
        FROM
          delta.`dbfs:/mnt/electricity-data/silver/generation_and_fuel_data`
        WHERE
          State IS NOT NULL
          AND Year IS NOT NULL
        GROUP BY
          Year,
          State
      )
    WHERE
      ROUND(Electric_Fuel_Consumption / Total_Fuel_Consumption, 3) BETWEEN 0.1 AND 0.9
    ORDER BY State, Year DESC)
```

```python
states = spark.sql(f"""
    SELECT DISTINCT state 
    FROM f923.generation_and_fuel_data
    WHERE state IS NOT NULL
    ORDER BY state
""")
# states = [st.state for st in states.collect() if not (st.state.startswith('M') or st.state.startswith('N'))]
states = [st.state for st in states.collect() if not st.state.startswith('M') and not st.state.startswith('N')]
print(states)
# dir(states)
```

```python
lower, upper = (0.1, 0.9)

sub_query = f"""
    SELECT
        year AS Year,
        state AS State,
        ROUND(SUM(net_generation_megawatthours), 1) AS Net_Generation,
        SUM(total_fuel_consumption_quantity) AS Total_Fuel_Consumption,
        SUM(electric_fuel_consumption_quantity) AS Electric_Fuel_Consumption
    FROM f923.generation_and_fuel_data
    WHERE State IS NOT NULL AND Year IS NOT NULL AND State IN {tuple(states)}
    GROUP BY Year, State
"""

sql_string = f"""
    SELECT
        Year,
        State,
        Net_Generation,
        ROUND(Electric_Fuel_Consumption/Total_Fuel_Consumption, 3) AS Percent_Electric_Consumption
    FROM ({sub_query})
    WHERE ROUND(Electric_Fuel_Consumption/Total_Fuel_Consumption, 3) BETWEEN {lower} AND {upper}
    ORDER BY Year, State, Percent_Electric_Consumption
""" 

df = spark.sql(sql_string)
display(df)
```