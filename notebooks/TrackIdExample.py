# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import pyseas
from pyseas import maps, styles
from pyseas.contrib import plot_tracks

# %matplotlib inline
# -

query = """
with 

-- Note that this query is reasonable expensive, so try to grab all the SSVID you want at
-- once, don't put this in a loop!

-- Grab the `track id`s and `aug_seg_id`s from the track table so that we can plot only
-- points on the tracks. `aug_seg_id`s are the normal seg_ids augmented with the date, so
-- segments that extend across multiple days will have different aug_seg_ids for each day.
track_id as (
  select seg_id as aug_seg_id, track_id
  from `gfw_tasks_1143_new_segmenter.tracks_v20200427_20191231`
  cross join unnest (seg_ids) as seg_id
  where ssvid in ('352894000' -- , other ssvid
                  )
    and (index = 0 or count > 1000)
),

-- Grab the messages from big query and add an aug_seg_id field which is constructed from
-- the seg_id and the date.
source_w_aug_seg_ids as (
    select *,
           concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
    from `pipe_production_v20200203.messages_segmented_*`
    where _TABLE_SUFFIX between "20170101" and "201912131"
),

-- Join the message source to voyages to the `track_id` table, matching
-- up messages to their `track_id` using `aug_seg_id`. Then join to
-- `voyages` and trim by start and end times so that we have just the
-- messages for each voyage mapped to `trip_id`
base as (
    select ssvid, seg_id, timestamp, lat, lon, course, speed, track_id, vessel_id
    from source_w_aug_seg_ids a
    join (select * from track_id)
    using (aug_seg_id)
    join
    (select seg_id, vessel_id from `world-fishing-827.pipe_production_v20200203.segment_info`
     group by seg_id, vessel_id)
     using (seg_id)
)


-- Select just the fields we want and order be timestamp.
select ssvid, seg_id, track_id, vessel_id,
        timestamp, lat, lon, speed, course
from base
order by timestamp
"""
ssvid_track_msgs = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')  

query = """
select * from `gfw_tasks_1143_new_segmenter.tracks_v20200427_20181231`
  where ssvid in ('352894000')
          and count > 1000
order by ssvid, index
"""
ssvid_tracks =  pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

# +
ssvid = '352894000'
df = ssvid_track_msgs[ssvid_track_msgs.ssvid == ssvid]
df_track = ssvid_tracks[ssvid_tracks.ssvid == ssvid]
for tid in set(df.track_id):
    n = (df.track_id == tid).sum()
    print(tid, n)
    names = df_track[df_track.track_id == tid].shipnames.iloc[0]
    names.sort(key=lambda x: x['count'], reverse=True)
    for x in names:
        if x['count'] < 15:
            break
        print('\t{} : {}'.format(x['value'], round(x['count'])))
        
df = ssvid_track_msgs[ssvid_track_msgs.ssvid == ssvid]
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/{}_tracks_panel.png'.format(ssvid), 
                dpi=300, facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()
# -

df = ssvid_track_msgs[ssvid_track_msgs.ssvid == ssvid]
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.vessel_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/{}_vessel_id_panel.png'.format(ssvid), 
                dpi=300, facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()
