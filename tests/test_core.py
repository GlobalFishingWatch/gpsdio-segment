"""
Unittests for gpsdio_segment.core
"""


from __future__ import division

import datetime

import gpsdio_segment.core


def test_msg_diff_stats():

    segmenter = gpsdio_segment.core.Segmenter([])

    msg1 = {
        'lat': 10,
        'lon': 10,
        'timestamp': datetime.datetime(2000, 1, 1, 0, 0, 0, 0)
    }
    msg2 = {
        'lat': 20,
        'lon': 20,
        'timestamp': datetime.datetime(2000, 1, 2, 12, 0, 0, 0)
    }

    # The method automatically figure out which message is newer and computes
    # a time delta accordingly.  Make sure this happens.
    stats = segmenter.msg_diff_stats(msg1, msg2)
    stats2 = segmenter.msg_diff_stats(msg2, msg1)
    assert stats == stats2

    assert round(stats['distance'], 0) == \
           round(segmenter._geod.inv(msg1['lon'], msg1['lat'],
                                     msg2['lon'], msg2['lat'])[2] / 1852, 0)

    assert stats['timedelta'] == 36

    assert stats['speed'] == stats['distance'] / stats['timedelta']
