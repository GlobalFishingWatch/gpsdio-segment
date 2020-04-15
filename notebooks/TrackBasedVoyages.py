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

-- Grab 100 random voyages
-- The where clause ensures that these  voyages are not be cut off at the 
-- beginning or end and are at least 12 hours long
voyages as (
select a.* except (track_id), track_id
from `machine_learning_dev_ttl_120d.track_based_voyages_v20200414` a
cross join unnest(track_id) as track_id
where trip_start_visit_id != 'NO_PREVIOUS_DATA'
  and trip_end_visit_id != 'ACTIVE_VOYAGE'
  and timestamp_diff(trip_end, trip_start, hour) > 12
order by farm_fingerprint(trip_id)
limit 100
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
    select *,
           concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
    from `pipe_production_v20200203.messages_segmented_2018*`
    where _TABLE_SUFFIX between "0101" and "0131"
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
select msgid, ssvid, type, seg_id, track_id, trip_id, trip_start, trip_end,
        timestamp, lat, lon, speed, course, heading
from base
order by timestamp
"""
pipeline_msgs = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')  

reload()
for trip_id in sorted(set(pipeline_msgs.trip_id)):
    df = pipeline_msgs[pipeline_msgs.trip_id == trip_id]
    plt.figure(figsize=(14, 14))
    with pyseas.context(styles.panel):
        df0 = df.iloc[0]
        plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, plots=[])
        maps.add_scalebar()
        plt.title('{}: {} – {}'.format(df0.ssvid, 
                    df0.trip_start.isoformat()[:-6], df0.trip_end.isoformat()[:-6]))
        plt.show()

# +
# import rendered
# rendered.publish_to_github('./TrackBasedVoyages.ipynb', 
#                            'gpsdio-segment/notebooks', action='push')
