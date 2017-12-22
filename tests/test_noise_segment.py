"""
Some debugging stuf.
"""

from datetime import datetime
from datetime import timedelta
import itertools as it
from collections import Counter

import pytest

import gpsdio
from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import NoiseSegment


def test_noise_segment():

    with gpsdio.open('tests/data/338013000.json') as src:
        # Run the whole thing - makes 31 segments, one of them real, the rest are singleton NoiseSegments
        segmentizer = Segmentizer(src)
        segs = [seg for seg in segmentizer]
        assert len(segs) == 31
        assert {len(seg) for seg in segs} == {1, 1223}
        assert Counter([seg.__class__.__name__ for seg in segs]) == {'Segment': 1, 'NoiseSegment': 30}

    with gpsdio.open('tests/data/338013000.json') as src:
        # now run it one day at a time and store the segment states in between
        seg_states = {}
        seg_types = {}
        for day, msgs in it.groupby(src, key=lambda x: x['timestamp'].day):
            prev_states = seg_states.get(day - 1)
            if prev_states:
                segmentizer = Segmentizer.from_seg_states(prev_states, msgs)
            else:
                segmentizer = Segmentizer(msgs)

            segs = [seg for seg in segmentizer]
            seg_types[day] = Counter([seg.__class__.__name__ for seg in segs])

            seg_states[day] = [seg.state for seg in segs if not isinstance(seg, NoiseSegment)]

        # 1 noise segment the first day that does not get passed back in on the second day
        assert seg_types == {18: {'Segment': 1, 'NoiseSegment': 1},
                             19: {'Segment': 1, 'NoiseSegment': 3},
                             20: {'Segment': 1, 'NoiseSegment': 26}}

        # should be 1 good segment on each day
        assert {day:len(states) for day, states in seg_states.iteritems()} == {18: 1, 19: 1, 20:1}