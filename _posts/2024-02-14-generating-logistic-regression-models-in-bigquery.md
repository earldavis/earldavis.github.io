---
layout: post
title: Using BigQuery for Data Science
date: 2024-02-14 09:55
categories: [data_science,]
description: A post about that time I used BigQuery to create a model to predict incidents in ServiceNow.
---

This is the SQL used to create a model in BigQuery to predict ServiceNow incidents based on time series data:

```sql
CREATE OR REPLACE MODEL `ms-gcp-g-i-aiops-d-dev.misc_dataset.mass_close_model_v4`
  TRANSFORM(time_opened,
    -- avg_count,
    network_incident,
    nagios_incident,
    autosys_incident,
    label,
    ML.STANDARD_SCALER(LOG(avg_count + 1)) OVER() AS scaled_log_avg_count,
    ML.STANDARD_SCALER(LOG(avg_count + 1)) OVER() * CAST(network_incident AS INT64) AS network_storm,
    ML.STANDARD_SCALER(LOG(avg_count + 1)) OVER() * CAST(nagios_incident AS INT64) AS nagios_storm,
    ML.STANDARD_SCALER(LOG(avg_count + 1)) OVER() * CAST(autosys_incident AS INT64) AS autosys_storm
  )
  OPTIONS(
    model_type='LOGISTIC_REG',
    input_label_cols=['label'],
    DATA_SPLIT_METHOD='SEQ',
    DATA_SPLIT_COL='time_opened',
    DATA_SPLIT_EVAL_FRACTION=0.25,
    NUM_TRIALS = 10,
    L2_REG=HPARAM_RANGE(0.1, 100)
    ) AS
  SELECT
    *
  FROM
    `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents-augmented`
  WHERE
    time_opened >= TIMESTAMP_SUB((SELECT DISTINCT TIMESTAMP('2023-07-01')
    FROM `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents-augmented`), INTERVAL 120 DAY)
```
