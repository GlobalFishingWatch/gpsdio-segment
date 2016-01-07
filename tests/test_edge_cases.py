"""
Tests for weird edge cases.
"""


from datetime import datetime
from datetime import timedelta

import pytest

from gpsdio_segment.core import Segmentizer


def test_first_is_non_posit():
    pass


def test_unsorted():
    before = {'mmsi': 1, 'timestamp': datetime.now(), 'lat': 90, 'lon': 90}
    after = {'mmsi': 1, 'timestamp': datetime.now(), 'lat': 90, 'lon': 90}
    with pytest.raises(ValueError):
        list(Segmentizer([after, before]))


def test_same_point_same_time():
    msg = {'mmsi': 100, 'lat': 10, 'lon': 10, 'timestamp': datetime.now()}
    segments = list(Segmentizer([msg, msg]))
    assert len(segments) == 1
    for seg in segments:
        assert len(seg) == 2


def test_same_point_absurd_timedelta():
    msg1 = {'mmsi': 10000, 'lat': -90, 'lon': -90, 'timestamp': datetime.now()}
    msg2 = {'mmsi': 10000, 'lat': -90, 'lon': -90,
            'timestamp': msg1['timestamp'] + timedelta(days=1000)}
    segments = list(Segmentizer([msg1, msg2]))
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 1


def test_with_non_posit():
    # Non-positional messages should be added to the segment that was last touched
    # This should produce two segments, each with 3 points - two of which are
    # positional and 1 that is a non-posit

    # Continuous
    msg1 = {'idx': 0, 'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    msg2 = {'idx': 1, 'mmsi': 1}
    msg3 = {'idx': 2, 'mmsi': 1, 'lat': 0.00001, 'lon': 0.00001,
            'timestamp': msg1['timestamp'] + timedelta(hours=12)}

    # Also continuous but not to the previous trio
    msg4 = {'idx': 3, 'mmsi': 1, 'lat': 65, 'lon': 65,
            'timestamp': msg3['timestamp'] + timedelta(days=100)}
    msg5 = {'idx': 4, 'mmsi': 1}
    msg6 = {'idx': 5, 'mmsi': 1, 'lat': 65.00001, 'lon': 65.00001,
            'timestamp': msg4['timestamp'] + timedelta(hours=12)}

    segments = list(Segmentizer([msg1, msg2, msg3, msg4, msg5, msg6]))
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 3


def test_first_message_out_of_bounds():

    """
    If the first input message has a location that is completely off the map,
    we need to make sure that it doesn't end up inside the internal segment
    container.
    """

    messages = [
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1)}
    ]

    output = list(Segmentizer(messages))
    assert len(output) == 2

    # Should get one bad segment and one good segment
    # Bad segment should just have the first message
    bs, s = output
    assert len(bs) == 1
    assert bs.msgs == messages[:1]

    # Good segment should have the rest of the messages
    assert len(s) == 3
    assert s.msgs == messages[1:]


def test_bad_message_in_stream():

    messages = [
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 1, 1)},
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1)}
    ]

    # Should get one bad segment and one good segment
    bs, s = list(Segmentizer(messages))

    assert len(s) == 3
    assert s.msgs == messages[:2] + messages[3:]

    assert len(bs) == 1
    assert bs.msgs == [messages[2]]
