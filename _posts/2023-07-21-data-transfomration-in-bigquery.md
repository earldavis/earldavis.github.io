---
layout: post
title: Using BigQuery for Data Engineering
date: 2023-07-21 11:06
categories: [data_science,]
description: A post about that time I used BigQuery to do some data transformation.
---

Here is some SQL code that I wrote to create a view in BigQuery. This view is used to augment a dataset of incidents with additional features. The view is used to train a machine learning model to predict whether an incident will be resolved within 15 minutes.

```sql
CREATE OR REPLACE VIEW `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents-augmented` AS
WITH cnt AS (
  SELECT number,
    COUNT(number) 
      OVER(PARTITION BY IFNULL(close_notes, "no notes") ORDER BY UNIX_SECONDS(TIMESTAMP(opened_at))
      RANGE BETWEEN 450 PRECEDING AND 450 FOLLOWING) AS close_count,
    COUNT(number) OVER(ORDER BY UNIX_SECONDS(TIMESTAMP(opened_at)) RANGE BETWEEN 30 PRECEDING
      AND 30 FOLLOWING) AS minute_count
  FROM `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents`
)
SELECT
  tbl.number,
  TIMESTAMP(tbl.opened_at) AS time_opened,
  CASE
    WHEN CONTAINS_SUBSTR(tbl.description, 'socket timeout') THEN TRUE
    WHEN CONTAINS_SUBSTR(tbl.description, 'nic redundancy') THEN TRUE
    WHEN CONTAINS_SUBSTR(tbl.description, 'ping failure') THEN TRUE
    WHEN CONTAINS_SUBSTR(tbl.description, 'networkredundancylostalarm') THEN TRUE
    ELSE FALSE
    END AS network_incident,
  IF(CONTAINS_SUBSTR(tbl.description, 'nagios'), TRUE, FALSE) AS nagios_incident,
  IF(CONTAINS_SUBSTR(tbl.description, 'autosys'), TRUE, FALSE) AS autosys_incident,
  MAX(cnt.minute_count) OVER(ORDER BY UNIX_SECONDS(TIMESTAMP(opened_at)) RANGE BETWEEN 450 PRECEDING
    AND 450 FOLLOWING) AS avg_count,
  tbl.description,
  tbl.close_notes,
  cnt.close_count,
  cnt.minute_count,
  IF(cnt.close_count >= 15, TRUE, FALSE) AS label
FROM
  `ms-gcp-g-i-aiops-d-dev.misc_dataset.incidents` AS tbl
LEFT JOIN cnt ON tbl.number = cnt.number
ORDER BY
  tbl.number ASC
```
