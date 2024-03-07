---
layout: post
title: Using BigQuery for Data Engineering
date: 2023-07-21 12:00
categories: [data_science,]
description: Creating a view in BigQuery for predictions.
---

A view that includes the predictions from a model:

```sql
CREATE OR REPLACE VIEW `misc_dataset.incidents-predicted-view` AS
SELECT 
  number, time_opened, predicted_label, 
  label, avg_count, network_incident,
  nagios_incident, autosys_incident
FROM ML.PREDICT(
  MODEL `ms-gcp-g-i-aiops-d-dev.misc_dataset.mass_close_model_v4`, (
    SELECT *
    FROM `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents-augmented`
    WHERE time_opened >= TIMESTAMP_SUB((SELECT MAX(time_opened)
    FROM `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents-augmented`), INTERVAL 3 DAY)
    ORDER BY time_opened)
  )
  ```
