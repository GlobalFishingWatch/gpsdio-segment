"""
Unittests for gpsdio_despoof.core
"""


from __future__ import division

import datetime

import gpsdio_despoof.core


def test_msg_diff_stats():

    despoofer = gpsdio_despoof.core.Despoofer([])

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
    stats = despoofer.msg_diff_stats(msg1, msg2)
    stats2 = despoofer.msg_diff_stats(msg2, msg1)
    assert stats == stats2

    assert round(stats['distance'], 0) == \
           round(despoofer._geod.inv(msg1['lon'], msg1['lat'],
                                     msg2['lon'], msg2['lat'])[2] / 1852, 0)

    assert stats['timedelta'] == 36

    assert stats['speed'] == stats['distance'] / stats['timedelta']
