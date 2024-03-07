---
layout: post
title: Using BigQuery for Data Engineering. Creating External Tables.
date: 2024-02-14 14:21
categories: [data_science,]
description: A post about that time I used BigQuery to do load an external parquet file into an external table.
---

Loading data from a bucket is easy using external tables in GCP:

```sql
CREATE OR REPLACE EXTERNAL TABLE `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents`
WITH PARTITION COLUMNS (ym_opened STRING)
OPTIONS(
  format="PARQUET",
  hive_partition_uri_prefix="gs://ms-gcp-g-i-aiops-d-dev-bucket/obfuscated-incidents.parquet",
  uris=["gs://ms-gcp-g-i-aiops-d-dev-bucket/obfuscated-incidents.parquet/*"]
)
```