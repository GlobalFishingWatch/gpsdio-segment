"""
Unittests for specific segmentation rules.
"""


from datetime import datetime
from datetime import timedelta

from click.testing import CliRunner

import gpsdio_segment.cli
from gpsdio_segment.core import Segmentizer


def test_inside_noise_distance_inside_time_infinite_speed():
    # If inside the noise distance and inside max time then point
    # should be added regardless of its inferred speed
    p1 = {'mmsi': 1, 'lat': 0, 'lon': 0, 'timestamp': datetime.now()}
    p2 = {'mmsi': 1, 'lat': 0.0000001, 'lon': 0.0000001, 'timestamp': datetime.now()}
    segmenter = Segmentizer([p1, p2])
    segments = list(segmenter)

    # Should produce a single segment containing two points
    stats = segmenter.msg_diff_stats(p1, p2)
    assert len(segments) == 1
    assert stats['distance'] <= segmenter.noise_dist
    assert stats['speed'] > segmenter.max_speed
    assert stats['timedelta'] <= segmenter.max_hours
    for seg in segments:
        assert len(seg) == 2


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


