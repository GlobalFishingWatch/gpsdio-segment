# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
import numpy as np
import pyseas
import matplotlib.pyplot as plt
from pyseas import styles, maps

# %matplotlib inline
# -

query = """
WITH

spoof_q as (
  SELECT
    ssvid,
    IF(NTILE(10) OVER (ORDER BY activity.overlap_hours_multinames) <= 9, 0, 1) as spoof_bucket
  FROM gfw_research.vi_ssvid_v20201101
),

activity_q as (
  SELECT
    ssvid,
    NTILE(2) OVER (ORDER by activity.active_positions) as activity_bucket
  FROM gfw_research.vi_ssvid_v20201101
),

region_q AS (
  SELECT ssvid, SUBSTR(ssvid, 1, 1) AS region_bucket
  FROM gfw_research.vi_ssvid_v20201101
  WHERE SUBSTR(ssvid, 1, 1) BETWEEN '2' AND '7'
),

class_q AS (
  SELECT ssvid,
         CASE 
           WHEN best.best_vessel_class NOT IN (
                  'tune_purse_seines',
                  'drifting_longlines',
                  'trawlers',
                  'squid_jigger',
                  'cargo',
                  'reefer')  THEN 'other'
           ELSE best.best_vessel_class
          END AS class_bucket
  FROM gfw_research.vi_ssvid_v20201101
  WHERE best.best_vessel_class IS NOT NULL
),

categorized AS (
  SELECT * 
  FROM spoof_q
  JOIN activity_q
  USING (ssvid)
  JOIN region_q
  USING (ssvid)
  JOIN class_q
  USING (ssvid)
),

filtered AS (
    SELECT *
    FROM categorized
    WHERE LENGTH(ssvid) = 9 
    AND SUBSTR(ssvid, 1, 1) NOT IN ('00', '99') -- Ignore probable base stations and buoys
),

counted AS (
  SELECT *, 
         ROW_NUMBER() OVER(PARTITION BY spoof_bucket, activity_bucket, region_bucket, class_bucket
                           ORDER BY FARM_FINGERPRINT(ssvid)) AS rn
  FROM filtered 
)


SELECT *
FROM counted 
WHERE rn = 1
"""
candidates = pd.read_gbq(query, project_id='world-fishing-827')

query = """
  SELECT ssvid, timestamp, lon, lat
  FROM `world-fishing-827.pipe_production_v20190502.position_messages_20200*` 
  WHERE ssvid in ({})
  order by ssvid, timestamp
""".format(','.join('"{}"'.format(x) for x in candidates.ssvid))
df = pd.read_gbq(query, project_id='world-fishing-827')

with pyseas.context(styles.light):
    fig = plt.figure(figsize=(16, 8))
    ax = maps.create_map()
    maps.add_land()
    for ssvid in df.ssvid.unique():
        mask = (df.ssvid == ssvid)
        plt.plot(df[mask].lon, df[mask].lat, '.', transform=maps.identity, markersize=2)

len(df.ssvid.unique()), len(candidates.ssvid.unique())

# To assemble final list, take the output above and dump it to a file, then add in the 
# "interesting" MMSI from https://docs.google.com/spreadsheets/d/1zn1lLGPykIYz5w6D7ii-bm_KXuCe_YiIsqm-9jk9RUE/edit#gid=0 .

# Comment out to avoid clobbering results after add MMSI from above spreadsheet
candidates['ssvid'].to_csv("../data/small_test_set.csv", index=False)

(candidates.ssvid.str.len() == 9).sum()


