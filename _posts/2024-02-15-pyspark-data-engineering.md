---
layout: post
title: Using Github Copilot for Data Engineering
date: 2024-02-15 14:09
categories: [data_science,]
description: A post about that time I used copilot to do some data engineering.
---

# DSSJU750

```python
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql import functions as F
```
# Start Spark session

```python
app_name = "DSSJU750"
builder = (SparkSession.builder.appName(app_name)
    .config('spark.jars.packages', 'org.apache.spark:spark-connect_2.12:3.4.2')
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.4.0')
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

spark = configure_spark_with_delta_pip(builder).getOrCreate()
root = '/path/to/the/project/work/'
write_path = root + "data/DSSJU750/"

# current_date = F.current_date()
current_date = '2023-11-21'
type_indicator = "BUD"
file_type = "parquet"
```

### JobPx750CreateBudgetFile
#### 824

```python
# Load and perform a transformation on the DataFrame
# df = spark.read.format("delta").load(root + "data/delta/DSS_DAY.delta")
df = spark.read.format(file_type).load(root + f"data/{file_type}/DSS_DAY.{file_type}")

# Will return a list of YR_PER values if there is more than one
yr_per = [i['YR_PER'] for i in (
    df
    .withColumn("YR_PER",
                F.when(F.substring(F.col("YR_PER"), 5, 2) == "13",
                       F.concat((F.substring(F.col("YR_PER"), 1, 4).cast("int") + 1).cast("string"),
                                F.lit("01"))).otherwise(F.col("YR_PER")))
    .filter(F.col("EOD_DT") == current_date)
    .collect()
)]

df_day = (
    df
    .select('FIN_EOW_DT', 'YR_PER')
    .distinct()
    .filter(F.col("YR_PER").isin(yr_per))
)
df_day.show()
```

```python
# ==============================================================================
# Superfluous to use the DSS_FIN_WK.delta table, but it's here for reference
# ==============================================================================
# fin_eow_dt_list = [row['FIN_EOW_DT'] for row in df_day.select("FIN_EOW_DT").collect()]
# fin_wk = (
#     spark.read.format("delta").load(root + "data/delta/DSS_FIN_WK.delta")
#     .filter(F.col("FIN_EOW_DT").isin(fin_eow_dt_list))
#     .sort("FIN_EOW_DT", ascending=True)
# )
# fin_wk.show()

# The filter condition appears to be an external variable of either "BUD" or "FOR"
# i.e. F.col("BUD_IND") == "BUD" or F.col("BUD_IND") == "FOR"
df = spark.read.format(file_type).load(root + f"data/{file_type}/DSS_CATG_BUD.{file_type}")
catg_bud = (
    df
    .filter((F.col("BUD_IND") == type_indicator) & (F.col("FOR_VER") == 0))
)
catg_bud.count()
```
```python
# ==============================================================================
# The join to fin_wk is superfluous, but it's here for reference.
# ==============================================================================
# The equivalent of the join is to use the df_day DataFrame instead of fin_wk.
# df = catg_bud.join(fin_wk, "FIN_EOW_DT", "inner")

# Perform a transformation on the DataFrame
df = (
    catg_bud
    .join(df_day, "FIN_EOW_DT", "inner")
    .select('DEPT_STORE_NBR', 'FIN_EOW_DT', 'BUD_SLS_AMT', 'YR_PER')
)
# print(df.count())
```
```python
df = df.withColumn("DEPT_STORE_NBR", F.col("DEPT_STORE_NBR").cast("long"))
df = df.withColumn("BUD_SLS_AMT", F.col("BUD_SLS_AMT").cast("decimal(12,4)"))
df = df.withColumn("YR_PER", F.col("YR_PER").cast("int"))

df = (
    df.groupBy("DEPT_STORE_NBR", "FIN_EOW_DT")
    .agg(F.sum("BUD_SLS_AMT").alias("BUD_SLS_AMT"), F.max("YR_PER").alias("YR_PER"))
)
# print(df.count())
```
```python
df = (
    df
    .groupBy("DEPT_STORE_NBR")
    .agg(F.mean("BUD_SLS_AMT").alias("BUD_SLS_AMT"), F.max("YR_PER").alias("YR_PER"))
)
# print(df.count())
```
```python
df = df.withColumn("CORP", F.lpad((F.col("DEPT_STORE_NBR") / 10000000).cast("int").cast("string"), 3, '0'))
df = df.withColumn("STORE", F.lpad((((F.col("DEPT_STORE_NBR") / 1000).cast("int") % 10000)).cast("string"), 4, '0'))
df = df.withColumn("DEPT", F.lpad((F.col("DEPT_STORE_NBR") % 1000).cast("string"), 3, '0'))
df = df.withColumn("BUD_SLS_AMT", F.when(F.col("BUD_SLS_AMT") < 0, 
                                                F.lpad(F.floor((F.col("BUD_SLS_AMT") * -1)).cast("string"), 8, '0'))
                                           .otherwise(F.lpad(F.floor(F.col("BUD_SLS_AMT")).cast("string"), 8, '0')))
df = df.withColumn("YR_PER", F.col("YR_PER").cast("string"))

cols = ["CORP", "STORE", "DEPT", "BUD_SLS_AMT", "YR_PER"]
df = (
    df
    .select(cols)
    .sort("CORP", "STORE", "DEPT")
)
# print(df.count())

df.show()
```
```python
# Write the DataFrame to a Delta table
df.write.format("delta").save(write_path + 'budgetfile.delta', mode='overwrite')
# Write the DataFrame to a Parquet file
df.write.format("parquet").save(write_path + 'budgetfile.parquet', mode='overwrite')
# Write the DataFrame to a CSV file
df.coalesce(1).write.format("csv").save(write_path + 'budgetfile.csv', header=True, mode='overwrite')
```
```python
# ==============================================================================
# Write the same text file as the original DSSJU750 program
# ==============================================================================
# Write the DataFrame to a "text" file
Budgetfile_FINAL = df.withColumn("con", F.concat_ws(',', *cols)).select('con')
# Budgetfile_FINAL.show()
Budgetfile_FINAL.coalesce(1).write.format("text").mode("overwrite").option("header", "true").save(write_path + 'budgetfile.txt')
```
```python
from delta.tables import *
import os

delta_read_path = os.path.join(write_path, "budgetfile.delta")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
DeltaTable.forPath(spark, delta_read_path).vacuum(0.01)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```