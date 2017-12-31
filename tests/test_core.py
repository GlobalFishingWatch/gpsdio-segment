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

    assert round(stats['distance'], 0) == round(
        segmenter._geod.inv(msg1['lon'], msg1['lat'], msg2['lon'], msg2['lat'])[2] / 1852, 0)

    assert stats['timedelta'] == 36
    assert stats['speed'] == stats['distance'] / stats['timedelta']

    # Create infinite speed
    msg1 = {'lat': -90, 'lon': -180, 'timestamp': datetime.datetime.now()}
    msg2 = {'lat': 90, 'lon': 180, 'timestamp': msg1['timestamp']}
    stats = segmenter.msg_diff_stats(msg1, msg2)
    assert stats['speed'] == gpsdio_segment.core.INFINITE_SPEED


def test_segmentizer_attrs():
    # Attributes that are not called during processing
    segmenter = gpsdio_segment.core.Segmentizer([])
    assert segmenter.__class__.__name__ in repr(segmenter)


def test_segment_attrs():
    seg = gpsdio_segment.core.Segment(1, 123456789)

    # Segment is empty
    assert seg.mmsi == 123456789
    assert seg.id == 1
    assert len(seg) == 0
    assert seg.last_point is None
    assert seg.last_msg is None

    # MMSI mismatch
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
        print(e)
        print(a)
        assert e == a

    passed = False
    for e, a in zip(expected_msgs, seg):
        passed = True
        assert e == a
    assert passed

    assert '4' in repr(seg)
    assert 'Segment' in repr(seg)


def test_last_msg_combinations():

    non_posit = {'mmsi': 1}
    posit = {'mmsi': 1, 'lat': 2, 'lon': 3}
    time_posit = {'mmsi': 1, 'lat': 2, 'lon': 3, 'timestamp': datetime.datetime.now()}

    seg = gpsdio_segment.core.Segment(0, mmsi=1)
    assert seg.last_msg is None

    seg.add_msg(non_posit)
    assert seg.last_msg == non_posit
    assert seg.last_time_posit_msg is None

    seg.add_msg(posit)
    assert seg.last_msg == posit
    assert seg.last_time_posit_msg is None

    seg.add_msg(time_posit)
    assert seg.last_msg == time_posit
    assert seg.last_time_posit_msg == time_posit

    # Make sure posit and posit time are being returned instead of just the last message
    seg = gpsdio_segment.core.Segment(1, mmsi=1)

    seg.add_msg(posit)
    seg.add_msg(non_posit)
    assert seg.last_msg == non_posit
    assert seg.last_time_posit_msg is None

    seg.add_msg(time_posit)
    seg.add_msg(non_posit)
    assert seg.last_msg == non_posit
    assert seg.last_time_posit_msg == time_posit

    seg.add_msg(time_posit)
    seg.add_msg(non_posit)
    assert seg.last_msg == non_posit
    assert seg.last_time_posit_msg == time_posit
