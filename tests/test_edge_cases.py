"""
Tests for weird edge cases.
"""


from datetime import datetime
from datetime import timedelta
from collections import Counter

import pytest

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import SegmentState


def test_first_is_non_posit():
    pass


def test_unsorted():
    before = {'mmsi': 1, 'timestamp': datetime.now(), 'lat': 90, 'lon': 90, 'course' : 0, 'speed': 1}
    after = {'mmsi': 1, 'timestamp': datetime.now(), 'lat': 90, 'lon': 90, 'course' : 0, 'speed': 1}
    with pytest.raises(ValueError):
        list(Segmentizer([after, before]))


def test_same_point_same_time():
    msg = {'mmsi': 100, 'lat': 10, 'lon': 10, 'timestamp': datetime.now(), 'course' : 0, 'speed': 1}
    segments = list(Segmentizer([msg, msg]))
    assert len(segments) == 1
    for seg in segments:
        assert len(seg) == 2


def test_same_point_absurd_timedelta():
    msg1 = {'mmsi': 10000, 'lat': -90, 'lon': -90, 'timestamp': datetime.now(), 'course' : 0, 'speed': 1}
    msg2 = {'mmsi': 10000, 'lat': -90, 'lon': -90,
            'timestamp': msg1['timestamp'] + timedelta(days=1000), 'course' : 0, 'speed': 1}
    segments = list(Segmentizer([msg1, msg2]))
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 1

def test_same_time_absurd_distance():
    t = datetime.now()
    msg1 = {'mmsi': 10000, 'lat': 0, 'lon': 0, 'timestamp': t, 'course' : 0, 'speed': 1}
    msg2 = {'mmsi': 10000, 'lat': 10, 'lon': 10, 'timestamp': t, 'course' : 0, 'speed': 1}
    segments = list(Segmentizer([msg1, msg2]))
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 1


def test_with_non_posit():
    # Non-positional messages should be added to the segment that was last touched
    # This should produce two segments, each with 3 points - two of which are
    # positional and 1 that is a non-posit

    # Continuous
    msg1 = {'idx': 0, 'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now(), 'course' : 0, 'speed': 1}
    msg2 = {'idx': 1, 'mmsi': 1, 'timestamp': msg1['timestamp'] + timedelta(hours=1), 'course' : 0, 'speed': 1}
    msg3 = {'idx': 2, 'mmsi': 1, 'lat': 0.00001, 'lon': 0.00001,
            'timestamp': msg1['timestamp'] + timedelta(hours=12), 'course' : 0, 'speed': 1}

    # Also continuous but not to the previous trio
    msg4 = {'idx': 3, 'mmsi': 1, 'lat': 65, 'lon': 65,
            'timestamp': msg3['timestamp'] + timedelta(days=100), 'course' : 0, 'speed': 1}
    msg5 = {'idx': 4, 'mmsi': 1, 'timestamp': msg4['timestamp'] + timedelta(hours=1), 'course' : 0, 'speed': 1}
    msg6 = {'idx': 5, 'mmsi': 1, 'lat': 65.00001, 'lon': 65.00001,
            'timestamp': msg4['timestamp'] + timedelta(hours=12), 'course' : 0, 'speed': 1}

    segments = list(Segmentizer([msg1, msg2, msg3, msg4, msg5, msg6]))
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 3


def test_with_non_posit_first():
    """
    non-pos message added first should be emitted as single message noise segment
    """
    messages = [
        {'mmsi': 1, 'timestamp': datetime(2015, 1, 1, 1, 1, 1)},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1), 'course' : 0, 'speed': 1}
    ]

    segs = list(Segmentizer(messages))
    assert len(segs) == 2


def test_first_message_out_of_bounds():

    """
    If the first input message has a location that is completely off the map,
    we need to make sure that it doesn't end up inside the internal segment
    container.
    """

    messages = [
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1), 'course' : 0, 'speed': 1}
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


def test_first_message_out_of_bounds_gt_24h():
    """
    Out of bounds location as teh first message after all previous segments have been cleared.
    Should put the bad message in a BadSegment and continue with the next good message
    """

    messages = [
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 2), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 10, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 10, 1, 1, 2), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 10, 1, 1, 3), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 10, 1, 1, 4), 'course' : 0, 'speed': 1}
    ]

    segs = list(Segmentizer(messages))
    assert Counter([seg.__class__.__name__ for seg in segs]) == {'Segment': 2, 'BadSegment': 1}


def test_non_pos_first_followed_by_out_of_bounds():
    """
    When a non-pos message is first, it gets emitted as a singleton segment,
    and then when a message with a bad location comes along
    it gets emitted as noise. Then a real segment is created.
    """
    messages = [
        {'mmsi': 1, 'timestamp': datetime(2015, 1, 1, 1, 1, 1)},
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 2), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 3), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 4), 'course' : 0, 'speed': 1}
    ]

    segs = list(Segmentizer(messages))
    assert Counter([seg.__class__.__name__ for seg in segs]) == {'InfoSegment': 1, 'BadSegment': 1, 'Segment': 1}


def test_bad_message_in_stream():

    messages = [
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 91, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 3, 1, 1, 1), 'course' : 0, 'speed': 1}
    ]

    # Should get one bad segment and one good segment
    bs, s = list(Segmentizer(messages))

    assert len(s) == 3
    assert s.msgs == messages[:2] + messages[3:]

    assert len(bs) == 1
    assert bs.msgs == [messages[2]]


def test_isssue_24_prev_state_nonpos_msg_gt_max_hours():

    messages1 = [
        {'mmsi': 1, 'lat': 89, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 1, 1, 1), 'course' : 0, 'speed': 1}
    ]
    messages2 = [
        {'mmsi': 1, 'shipname': 'Boaty', 'timestamp': datetime(2015, 1, 9, 1, 1, 1)}
    ]

    seg_states = [seg.state for seg in Segmentizer.from_seg_states(seg_states=[], instream=messages1)]

    # these two should should produce the same result
    seg_msg_count1 = [len(seg.msgs) for seg in list(Segmentizer.from_seg_states(seg_states=seg_states, instream=messages2))]
    seg_msg_count2 = [len(seg.msgs) for seg in Segmentizer.from_seg_states(seg_states=seg_states, instream=messages2)]

    expected = [0, 1]
    assert expected == seg_msg_count1
    assert expected == seg_msg_count2


def test_max_hours_exceeded_with_non_pos_message():
    messages = [
        {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime(2015, 1, 1, 0, 0, 0), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'shipname': 'Boaty', 'timestamp': datetime(2015, 1, 1, 12, 0, 0), 'course' : 0, 'speed': 1},
        {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime(2015, 1, 2, 1, 0, 0), 'course' : 0, 'speed': 1}
    ]

    seg_msg_count = [len(seg) for seg in Segmentizer(messages, max_hours=24)]
    assert seg_msg_count == [2, 1]


def test_duplicate_pos_msg():
    msg1 = {'mmsi': 1, 'lat': 21.42061667, 'lon': -91.77805, 'timestamp': datetime(2016, 5, 1, 0, 31, 27), 'course' : 0, 'speed': 0}
    msg2 = {'mmsi': 1, 'lat': 21.45295, 'lon': -91.80513333, 'timestamp': datetime(2016, 5, 1, 1, 31, 27), 'course' : 0, 'speed': 0}

    segments = list(Segmentizer([msg1, msg1, msg1, msg1, msg2]))
    assert len(segments) == 1
    for seg in segments:
        assert len(seg) == 5

def test_duplicate_ts_multiple_segs():
    # example from mmsi 316004240 2018-05-18 to 2018-05-19
    # 2 segments present because of a noise position in idx=1
    # so we have 2 segments [0,2,3,4] and [1] when 4 comes along.
    messages = [
        {'idx': 0, 'mmsi': 1, 'lat': 44.63928, 'lon': -63.551333, 'timestamp': datetime(2018, 5, 18, 14, 40, 12), 'course' : 0, 'speed': 1},
        {'idx': 1, 'mmsi': 1, 'lat': 51.629493, 'lon': -63.55381, 'timestamp': datetime(2018, 5, 18, 14, 43, 8), 'course' : 0, 'speed': 1},
        {'idx': 2, 'mmsi': 1, 'lat': 44.63896, 'lon': -63.55386, 'timestamp': datetime(2018, 5, 18, 14, 43, 16), 'course' : 0, 'speed': -1},
        {'idx': 3, 'mmsi': 1, 'lat': 44.573973, 'lon': -63.534027, 'timestamp': datetime(2018, 5, 19, 7, 48, 12), 'course' : 0, 'speed': 1},
        {'idx': 4, 'mmsi': 1, 'lat': 44.583315, 'lon': -63.533645, 'timestamp': datetime(2018, 5, 19, 7, 48, 12), 'course' : 0, 'speed': 1},
    ]

    segments = list(Segmentizer(messages))
    assert [{0, 2, 3, 4}, {1}]== [{msg['idx'] for msg in seg} for seg in segments]
