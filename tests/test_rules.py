"""
Unittests for specific segmentation rules.
"""


from datetime import datetime
from datetime import timedelta

from click.testing import CliRunner

import gpsdio_segment.cli
from gpsdio_segment.core import Segmentizer

# deltas = [{'distance': 0, 'speed': 0, 'duration': 0}]
# distances are in nautical miles
# speeds are in knots
# durations are in hours
# specify 2 of the 3 possible values
def generate_messages_from_deltas(deltas):
    msg = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    yield msg
    for d in deltas:
        distance = d.get('distance') or d['speed'] * d['duration']
        duration = d.get('duration') or d['speed'] / d['distance']
        lon = msg['lon'] + (distance / 60.0)
        ts = msg['timestamp'] + timedelta(hours=duration)
        msg = dict(mmsi=msg['mmsi'], lat=msg['lat'], lon=lon, timestamp=ts)
        yield msg

def test_noise_distance():
    # adding a point inside noise distance but with too high max speed should result in the
    # point being emitted in a separate segment
    deltas = [
        {'duration': 1, 'speed': 5},
        {'duration': 1, 'speed': 5},
        {'duration': 1, 'speed': 5},
        {'duration': 0.1, 'speed': 100},
        {'duration': 0.1, 'speed': 5},
        ]

    messages = list(generate_messages_from_deltas(deltas))
    segmenter = Segmentizer(messages, noise_dist=30)
    segments = list(segmenter)
    assert {len(s.msgs) for s in segments} == {4, 1, 1}

    segmenter = Segmentizer(messages, noise_dist=0)
    segments = list(segmenter)
    assert {len(s.msgs) for s in segments} == {4, 2}


def test_two_different_mmsi():
    # If a second different MMSI is encountered it should be ignored
    # Should produce a single segment containing a single point
    p1 = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 2, 'lat': 0.0000001, 'lon': 0.0000001, 'timestamp': datetime.now()}
    segmenter = Segmentizer([p1, p2])
    segments = list(segmenter)

    # Should produce a single segment containing a single point
    assert len(segments) == 1
    for seg in segments:
        assert len(seg) == 1


def test_good_speed_good_time():
    # Make sure two points within the max_hours and max_speed are in the same segment
    p1 = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 1, 'lat': 1, 'lon': 1, 'timestamp': p1['timestamp'] + timedelta(hours=12)}
    segmenter = Segmentizer([p1, p2])
    segments = list(segmenter)

    # Should produce a single segment with two points
    stats = segmenter.msg_diff_stats(p1, p2)
    assert stats['speed'] <= segmenter.max_speed
    assert stats['timedelta'] <= segmenter.max_hours
    assert len(segments) == 1
    for seg in segments:
        assert len(seg) == 2


def test_good_speed_bad_time():
    # Two points within a reasonable distance but an out of bounds time
    p1 = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 1, 'lat': 1, 'lon': 1, 'timestamp': p1['timestamp'] + timedelta(days=10)}
    segmenter = Segmentizer([p1, p2])
    segments = list(segmenter)

    # Should produce a two segments each with a single point
    stats = segmenter.msg_diff_stats(p1, p2)
    assert stats['speed'] <= segmenter.max_speed
    assert stats['timedelta'] > segmenter.max_hours
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 1


def test_bad_speed_good_time():
    # Two points outside the noise distance with a speed that is far too great
    # but within a reasonable time delta
    p1 = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 1, 'lat': 10, 'lon': 10, 'timestamp': p1['timestamp'] + timedelta(hours=12)}
    segmenter = Segmentizer([p1, p2])
    segments = list(segmenter)

    # Should produce two segments, each with a single point
    stats = segmenter.msg_diff_stats(p1, p2)
    assert stats['speed'] > segmenter.max_speed
    assert stats['timedelta'] <= segmenter.max_hours
    assert len(segments) == 2
    for seg in segments:
        assert len(seg) == 1


