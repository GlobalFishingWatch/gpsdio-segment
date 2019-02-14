"""
Unittests for message type rules
"""

import pytest

from datetime import datetime
from datetime import timedelta
from itertools import groupby

from click.testing import CliRunner

import gpsdio_segment.cli
from gpsdio_segment.core import Segmentizer


@pytest.mark.parametrize("type,expected", [
    (1, 'A'),
    (5, 'A'),
    (18, 'B'),
    (99, None),
])
def test_message_type(type, expected):
    segmenter = Segmentizer([])
    assert segmenter.message_type({'type': type}) == expected

@pytest.mark.parametrize("type1,type2,expected", [
    (1,1,False),
    (1,18,True),
    (1,99,None),
])
def test_one_seg_message_type(type1, type2, expected):
    # test two points in one segment with the same message type
    p1 = {'mmsi': 1, 'type': type1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 1, 'type': type2, 'lat': 0, 'lon': 0, 'timestamp': p1['timestamp'] + timedelta(hours=1)}
    segmenter = Segmentizer([p1, p2])
    assert segmenter.msg_diff_stats(p1, p2)['type_mismatch'] == expected
    # Should produce one segment
    assert len(list(segmenter)) == 1

def test_two_seg_same_message_type():
    # test several points in two segments all with the same message type
    t = datetime.now()
    points = [
        {'mmsi': 1, 'type': 1, 'lat': 0.0, 'lon': 0, 'timestamp': t},
        {'mmsi': 1, 'type': 1, 'lat': 2.0, 'lon': 0, 'timestamp': t + timedelta(hours=1)},
        {'mmsi': 1, 'type': 1, 'lat': 0.5, 'lon': 0, 'timestamp': t + timedelta(hours=2)},
        {'mmsi': 1, 'type': 1, 'lat': 1.5, 'lon': 0, 'timestamp': t + timedelta(hours=3)},
        {'mmsi': 1, 'type': 1, 'lat': 1.0, 'lon': 0, 'timestamp': t + timedelta(hours=4)}
    ]
    # Should produce two segments, with lat=1.0 grouped with lat=1.5
    segments = list(Segmentizer(points))
    assert {tuple(msg['lat'] for msg in seg) for seg in segments} == {(2.0, 1.5, 1.0),(0.0, 0.5)}


def test_two_seg_diff_message_type():
    # test several points in two segments with the different message types
    t = datetime.now()
    points = [
        {'mmsi': 1, 'type': 1, 'lat': 0.0, 'lon': 0, 'timestamp': t},
        {'mmsi': 1, 'type': 18, 'lat': 2.0, 'lon': 0, 'timestamp': t + timedelta(hours=1)},
        {'mmsi': 1, 'type': 1, 'lat': 0.5, 'lon': 0, 'timestamp': t + timedelta(hours=2)},
        {'mmsi': 1, 'type': 18, 'lat': 1.5, 'lon': 0, 'timestamp': t + timedelta(hours=3)},
        {'mmsi': 1, 'type': 1, 'lat': 1.0, 'lon': 0, 'timestamp': t + timedelta(hours=4)}
    ]
    # Should produce two segments, with lat=1.0 grouped with lat=0.5
    segments = list(Segmentizer(points))
    assert {tuple(msg['lat'] for msg in seg) for seg in segments} == {(2.0, 1.5),(0.0, 0.5, 1.0)}


def test_ident_msg_24():
    # test several points in two segments with the different message types
    t = datetime.now()
    points = [
        {'id': 1, 'mmsi': 1, 'type': 1, 'lat': 0.0, 'lon': 0, 'timestamp': t},
        {'id': 2, 'mmsi': 1, 'type': 18, 'lat': 2.0, 'lon': 0, 'timestamp': t + timedelta(hours=1)},
        {'id': 3, 'mmsi': 1, 'type': 1, 'lat': 0.5, 'lon': 0, 'timestamp': t + timedelta(hours=2)},
        {'id': 4, 'mmsi': 1, 'type': 18, 'lat': 1.5, 'lon': 0, 'timestamp': t + timedelta(hours=3)},
        {'id': 5, 'mmsi': 1, 'type': 24, 'timestamp': t + timedelta(hours=4)}
    ]
    # Should produce two segments, with ident message 5 grouped with messages 2 and 4
    segments = list(Segmentizer(points))
    assert {tuple(msg['id'] for msg in seg) for seg in segments} == {(2, 4, 5),(1, 3)}

def test_ident_msg_5():
    # test several points in two segments with the different message types
    t = datetime.now()
    points = [
        {'id': 1, 'mmsi': 1, 'type': 1, 'lat': 0.0, 'lon': 0, 'timestamp': t},
        {'id': 2, 'mmsi': 1, 'type': 18, 'lat': 2.0, 'lon': 0, 'timestamp': t + timedelta(hours=1)},
        {'id': 3, 'mmsi': 1, 'type': 1, 'lat': 0.5, 'lon': 0, 'timestamp': t + timedelta(hours=2)},
        {'id': 4, 'mmsi': 1, 'type': 18, 'lat': 1.5, 'lon': 0, 'timestamp': t + timedelta(hours=3)},
        {'id': 5, 'mmsi': 1, 'type': 5, 'timestamp': t + timedelta(hours=4)}
    ]
    # Should produce two segments, with ident message 5 grouped with messages 2 and 4
    segments = list(Segmentizer(points))
    assert {tuple(msg['id'] for msg in seg) for seg in segments} == {(2, 4),(1, 3,5)}


@pytest.mark.parametrize("label,message_stubs", [
    (   'class_none',     # test matching when the latest position message does not have a defined class
        [{'seg': 0, 'type': 1, 'lat': 0.0, 'lon': 0},
        {'seg': 1, 'type': 1, 'lat': 2.0, 'lon': 0},
        {'seg': 0, 'type': 1, 'lat': 0.5, 'lon': 0},
        {'seg': 1, 'type': 3, 'lat': 1.5, 'lon': 0},
        {'seg': 1, 'type': 1, 'lat': 1.5, 'lon': 0},
    ]),
])
def test_class_none(label, message_stubs, msg_generator):

    msg_generator.assert_segments(message_stubs, label='test_class_none')
