"""
Unittests for gpsdio_segment.core
"""


from __future__ import division

import datetime

import pytest

import gpsdio_segment.core


def test_msg_diff_stats():

    segmenter = gpsdio_segment.core.Segmentizer([])

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


def test_segment_attrs():
    seg = gpsdio_segment.core.Segment(1, 123456789)

    # Segment is empty
    assert seg.mmsi == 123456789
    assert seg.id == 1
    assert len(seg) == 0
    with pytest.raises(IndexError):
        seg.last_point
    with pytest.raises(IndexError):
        seg.last_msg
    with pytest.raises(ValueError):
        seg.add_msg({'mmsi': 1})

    # Add some data
    msg1 = {'mmsi': 123456789, 'field': 100}
    msg2 = {'mmsi': 123456789, 'field': 100000}
    msg_with_point1 = {'mmsi': 123456789, 'lat': 1, 'lon': 1}
    msg_with_point2 = {'mmsi': 123456789, 'lat': 10, 'lon': 10}

    seg.add_msg(msg1)
    seg.add_msg(msg2)
    assert len(seg) == 2 == len(seg.msgs)
    assert len(seg.coords) == 0
    assert seg.last_msg == msg2

    seg.add_msg(msg_with_point1)
    seg.add_msg(msg_with_point2)
    assert len(seg) == 4 == len(seg.msgs)
    assert len(seg.coords) == 2
    assert seg.last_point == (msg_with_point2['lon'], msg_with_point2['lat'])
    assert seg.bounds == (1, 1, 10, 10)

    expected_msgs = [msg1, msg2, msg_with_point1, msg_with_point2]
    for e, a in zip(expected_msgs, seg):
        assert e == a

    # TODO: Iterate twice