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
import matplotlib.colors as mpcolors
import skimage.io
import pandas as pd
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


# We are importing extra stuff here and defining a reload function to
# make iterative testing of PySeas easier. You should not need to use
# `reload` during normal use.
import pyseas
from pyseas import maps, cm, styles, util
import pyseas.props
from pyseas.contrib import plot_tracks
from pyseas.maps import scalebar, core, rasters, ticks
import imp

def reload():
    imp.reload(util)
    imp.reload(ticks)
    imp.reload(scalebar)
    imp.reload(pyseas.props)
    imp.reload(cm)
    imp.reload(styles)
    imp.reload(rasters)
    imp.reload(core)
    imp.reload(maps)
    imp.reload(plot_tracks)
    imp.reload(pyseas)
reload()

# %matplotlib inline
# -

query = """
with 

-- This query is moderately expensive, so if you are plotting multiple vessels, it's 
-- best to grab them all at once using the in statement below than plotting them one at
-- a time.

-- Grab all voyages from '352894000' and '567391000'
voyages as (
select *
from `machine_learning_dev_ttl_120d.track_based_voyages_v20200424b` a
where ssvid in ('352894000', '567391000', '377288000', '413700050')
order by farm_fingerprint(trip_id)
),

-- Grab the `track id`s and `aug_seg_id`s from the track table so that we can plot only
-- points on the tracks. `aug_seg_id`s are the normal seg_ids augmented with the date, so
-- segments that extend across multiple days will have different aug_seg_ids for each day.
track_id as (
  select seg_id as aug_seg_id, track_id
  from `gfw_tasks_1143_new_segmenter.tracks_v20200229d_20191231`
  cross join unnest (seg_ids) as seg_id
),

-- Grab the messages from big query and add an aug_seg_id field which is constructed from
-- the seg_id and the date.
source_w_aug_seg_ids as (
    select ssvid, seg_id, timestamp, lat, lon, course, speed,
           concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
    from `pipe_production_v20200203.messages_segmented_2018*`
--    where _TABLE_SUFFIX between "0101" and "0131"
),

-- Join the message source to voyages to the `track_id` table, matching
-- up messages to their `track_id` using `aug_seg_id`. Then join to
-- `voyages` and trim by start and end times so that we have just the
-- messages for each voyage mapped to `trip_id`
base as (
    select a.*, track_id, trip_id, trip_start, trip_end
    from source_w_aug_seg_ids a
    join (select * from track_id)
    using (aug_seg_id)
    join voyages
    using (track_id)
    where a.timestamp between trip_start and trip_end
)


-- Select just the fields we want and order be timestamp.
select ssvid, seg_id, track_id, trip_id, trip_start, trip_end,
        timestamp, lat, lon, speed, course
from base
order by timestamp
"""
pipeline_msgs = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')  

df = pipeline_msgs[pipeline_msgs.ssvid == '413700050']
for tid in set(df.trip_id):
    n = (df.trip_id == tid).sum()
    print(tid, n)
print()
print(sorted(set(df.track_id)))

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '413700050']
print('plotting 10 of', len(set(df_ssvid.trip_id)), 'voyages')
starts = {x.trip_id : x.trip_start for x in df_ssvid.itertuples()}
for trip_id in sorted(set(df_ssvid.trip_id), key=lambda x: starts[x])[:10]:
    df = df_ssvid[df_ssvid.trip_id == trip_id]
    plt.figure(figsize=(14, 14))
    with pyseas.context(styles.panel):
        df0 = df.iloc[0]
        plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, plots=[], )
#         maps.add_scalebar(skip_when_extent_large=True)
        plt.title('{}: {} – {}'.format(df0.ssvid, 
                    df0.trip_start.isoformat()[:-6], df0.trip_end.isoformat()[:-6]))
        plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '377288000']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df_ssvid.iloc[0]
    plot_tracks.plot_tracks_panel(df_ssvid.timestamp, df_ssvid.lon, df_ssvid.lat, df_ssvid.trip_id, plots=[])
    plt.title('{}'.format(df0.ssvid))
    plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '377288000']
starts = {x.trip_id : x.trip_start for x in df_ssvid.itertuples()}
for trip_id in sorted(set(df_ssvid.trip_id), key=lambda x: starts[x]):
    df = df_ssvid[df_ssvid.trip_id == trip_id]
    plt.figure(figsize=(14, 14))
    with pyseas.context(styles.panel):
        df0 = df.iloc[0]
        plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, plots=[], )
#         maps.add_scalebar(skip_when_extent_large=True)
        plt.title('{}: {} – {}'.format(df0.ssvid, 
                    df0.trip_start.isoformat()[:-6], df0.trip_end.isoformat()[:-6]))
        plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '352894000']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df_ssvid.iloc[0]
    plot_tracks.plot_tracks_panel(df_ssvid.timestamp, df_ssvid.lon, df_ssvid.lat, df_ssvid.trip_id, plots=[])
    plt.title('{}'.format(df0.ssvid))
    plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '352894000']
print('plotting 10 of', len(set(df_ssvid.trip_id)), 'voyages')
starts = {x.trip_id : x.trip_start for x in df_ssvid.itertuples()}
for trip_id in sorted(set(df_ssvid.trip_id), key=lambda x: starts[x])[:10]:
    df = df_ssvid[df_ssvid.trip_id == trip_id]
    plt.figure(figsize=(14, 14))
    with pyseas.context(styles.panel):
        df0 = df.iloc[0]
        plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, plots=[], )
#         maps.add_scalebar(skip_when_extent_large=True)
        plt.title('{}: {} – {}'.format(df0.ssvid, 
                    df0.trip_start.isoformat()[:-6], df0.trip_end.isoformat()[:-6]))
        plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '567391000']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df_ssvid.iloc[0]
    plot_tracks.plot_tracks_panel(df_ssvid.timestamp, df_ssvid.lon, df_ssvid.lat, df_ssvid.trip_id, plots=[])
    plt.title('{}'.format(df0.ssvid))
    plt.show()

reload()
df_ssvid = pipeline_msgs[pipeline_msgs.ssvid == '567391000']
trip_ids = sorted(set(df_ssvid.trip_id))
starts = {x.trip_id : x.trip_start for x in df_ssvid.itertuples()}
for trip_id in sorted(set(df_ssvid.trip_id), key=lambda x: starts[x]):
    df = df_ssvid[df_ssvid.trip_id == trip_id]
    plt.figure(figsize=(14, 14))
    with pyseas.context(styles.panel):
        df0 = df.iloc[0]
        plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, plots=[], )
#         maps.add_scalebar(skip_when_extent_large=True)
        plt.title('{}: {} – {}'.format(df0.ssvid, 
                    df0.trip_start.isoformat()[:-6], df0.trip_end.isoformat()[:-6]))
        plt.show()



# ## Investigate  '352894000'
#
# ~According to Hannah, '352894000' should have may voyages in 2018, yet we are seeing only 2, one
# at the end of the year and one at the beginning.~ [Now fixed, but leaving here as an example of
# plotting tracks]
#
# Step 1. Plot the track for '352894000' to see if it's a track issue.

query = """
with 

-- Grab the `track id`s and `aug_seg_id`s from the track table so that we can plot only
-- points on the tracks. `aug_seg_id`s are the normal seg_ids augmented with the date, so
-- segments that extend across multiple days will have different aug_seg_ids for each day.
track_id as (
  select seg_id as aug_seg_id, track_id
  from `gfw_tasks_1143_new_segmenter.tracks_v20200229d_20191231`
  cross join unnest (seg_ids) as seg_id
  where ssvid in ('352894000', '567391000', '377288000', '413700050')
),

-- Grab the messages from big query and add an aug_seg_id field which is constructed from
-- the seg_id and the date.
source_w_aug_seg_ids as (
    select *,
           concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
    from `pipe_production_v20200203.messages_segmented_2018*`
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

df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '413700050']
for tid in set(df.track_id):
    n = (df.track_id == tid).sum()
    print(tid, n)

reload()
# df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '413700050']
df = ssvid_track_msgs[ssvid_track_msgs.track_id == '413700050-2017-10-27T19:36:40.000000Z-2017-10-27']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/413700050_tracks_panel.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()

reload()
plt.figure(figsize=(14, 14))
df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '413700050']
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.vessel_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/413700050_tracks_panel_vessel_id.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()

df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '352894000']
for tid in set(df.track_id):
    n = (df.track_id == tid).sum()
    print(tid, n)

reload()
df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '352894000']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/352894000_tracks_panel.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()


# Figure out an appropriate extent for viewing just the second track
reload()
df = ssvid_352894000_track_msgs[ssvid_352894000_track_msgs.track_id == 
                                '352894000-2012-01-03T00:11:45.000000Z-2012-01-03']
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info1 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id,
                            projection_info=info1.projection_info, plots={})
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.show()

# Now use that projection to display the original plot (so with same colors, etc)
# in a way that centers on the second track.
reload()
df = ssvid_352894000_track_msgs
plt.figure(figsize=(14, 8))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    _ = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id,
                            projection_info=info1.projection_info, plots={})
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/352894000_tracks_panel_indonesia.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()

reload()
df = ssvid_352894000_track_msgs
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.vessel_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/352894000_tracks_panel_vessel_id.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()

reload()
df = ssvid_track_msgs[ssvid_track_msgs.ssvid == '377288000']
plt.figure(figsize=(14, 14))
with pyseas.context(styles.panel):
    df0 = df.iloc[0]
    info0 = plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id)
    maps.add_scalebar(skip_when_extent_large=True)
    plt.title('{}'.format(df0.ssvid))
    plt.savefig('/Users/timothyhochberg/Desktop/377288000_tracks_panel.png', dpi=300,
               facecolor=plt.rcParams['pyseas.fig.background'])
    plt.show()

# +
# import rendered
# rendered.publish_to_github('./TrackBasedVoyages.ipynb', 
#                            'gpsdio-segment/notebooks', action='push')
# -


